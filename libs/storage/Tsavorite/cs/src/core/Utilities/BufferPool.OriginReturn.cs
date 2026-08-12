// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
#define CHECK_FREE      // disabled by default in Release due to overhead; must match BufferPool.cs
#endif

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Standalone byte-budget accounting for the origin-return backend. Referenced directly by the pool and by
    /// every permit-holding buffer/shard so a late permit release (e.g. from a retired shard's finalizer)
    /// remains valid after the pool is closed. The counter is only touched on a buffer's cacheable birth and
    /// on its permanent death — never on the hot local/xthread/depot transitions.
    /// </summary>
    internal sealed class BudgetState
    {
        internal readonly long budgetBytes;
        private long usedBytes;

        internal BudgetState(long budgetBytes) => this.budgetBytes = budgetBytes;

        /// <summary>Reserve <paramref name="bytes"/> against the budget; false if it would exceed the cap.</summary>
        internal bool TryReserve(long bytes)
        {
            while (true)
            {
                var cur = Volatile.Read(ref usedBytes);
                var next = cur + bytes;
                if (next > budgetBytes)
                    return false;
                if (Interlocked.CompareExchange(ref usedBytes, next, cur) == cur)
                    return true;
            }
        }

        /// <summary>Release a previously reserved <paramref name="bytes"/>.</summary>
        internal void Release(long bytes) => Interlocked.Add(ref usedBytes, -bytes);

        internal long Used => Volatile.Read(ref usedBytes);
    }

    /// <summary>
    /// One <c>(pool, thread, size-class)</c> cache. Owner-only fields (<see cref="localHead"/> etc.) are touched
    /// without atomics by the owning thread; the cross-thread <see cref="xthreadHead"/> is a lock-free Treiber
    /// stack that other (IO-completion) threads push onto and the owner batch-claims. The two groups are placed
    /// on separate cache lines (padding fields) to avoid false sharing. When the owning shard retires,
    /// <see cref="xthreadHead"/> is set to the <see cref="SectorAlignedBufferPool.Sealed"/> sentinel so late
    /// foreign Returns reroute to the depot instead of stranding.
    /// </summary>
    internal sealed class Bucket
    {
#pragma warning disable CS0169 // padding fields are intentionally unused (false-sharing separation)
        // Owner-only cache line.
        internal SectorAlignedMemory localHead;
        internal int localCount;
        internal long localBytes;
        private long pad0, pad1, pad2, pad3, pad4, pad5, pad6;

        // Cross-thread cache line.
        internal SectorAlignedMemory xthreadHead;
        private long pad7, pad8, pad9, pad10, pad11, pad12, pad13;
#pragma warning restore CS0169

        // Immutable identity.
        internal ThreadShard owner;
        internal int sizeClass;
        internal long classCapacityBytes;
    }

    /// <summary>
    /// Per <c>(pool, thread)</c> shard owning one <see cref="Bucket"/> per size-class. Finalizable so that a
    /// thread which dies with buffers still cached releases those buffers' budget permits (there is no
    /// thread-exit callback). Self-clears its heavy references on seal so a lingering thread-static slot on
    /// another thread roots nothing large.
    /// </summary>
    internal sealed class ThreadShard
    {
        internal const int Alive = 0;
        internal const int Sealed = 1;

        internal Bucket[] buckets;
        internal SectorAlignedBufferPool pool;      // identity check for a recycled thread-static slot; nulled on seal
        internal readonly BudgetState budget;       // held directly so the finalizer can release permits post-close
        internal int state;                          // Alive / Sealed
        internal int drainedOnce;                    // CAS 0->1: arbitrates explicit seal vs. finalization (drain at most once)

        internal ThreadShard(SectorAlignedBufferPool pool, int numClasses, BudgetState budget, int sectorSize, int[] classCaps, bool bornSealed)
        {
            this.pool = pool;
            this.budget = budget;
            state = bornSealed ? Sealed : Alive;
            buckets = new Bucket[numClasses];
            for (var c = 0; c < numClasses; c++)
            {
                var bucket = new Bucket
                {
                    owner = this,
                    sizeClass = c,
                    classCapacityBytes = (long)classCaps[c] * sectorSize
                };
                if (bornSealed)
                    bucket.xthreadHead = SectorAlignedBufferPool.Sealed;   // late foreign Returns reroute to depot
                buckets[c] = bucket;
            }
        }

        /// <summary>
        /// Finalizer: the owning thread is dead and no in-flight buffer roots this shard, so release the budget
        /// permits of the buffers it still holds. Arbitrated with explicit sealing via <see cref="drainedOnce"/>.
        /// </summary>
        ~ThreadShard()
        {
            if (Interlocked.CompareExchange(ref drainedOnce, 1, 0) != 0)
                return;
            var localBuckets = buckets;
            if (localBuckets is null)
                return;
            for (var c = 0; c < localBuckets.Length; c++)
            {
                var bucket = localBuckets[c];
                if (bucket is null)
                    continue;
                var chain = Interlocked.Exchange(ref bucket.xthreadHead, SectorAlignedBufferPool.Sealed);
                if (!ReferenceEquals(chain, SectorAlignedBufferPool.Sealed))
                    SectorAlignedBufferPool.ReleaseChainPermits(chain);
                SectorAlignedBufferPool.ReleaseChainPermits(bucket.localHead);
                bucket.localHead = null;
            }
            buckets = null;
        }
    }

    /// <summary>
    /// Pool-owned, per-size-class, lightly-striped overflow depot. A cold path (only hit when a thread's local
    /// and cross-thread lists are both empty), so a per-stripe lock is fine and sidesteps ABA entirely. The
    /// close flag is consulted under the same lock as push, so there is no lifecycle-check-then-enqueue race.
    /// </summary>
    internal sealed class DepotStripe
    {
        private readonly Stack<SectorAlignedMemory> items = new();
        private readonly int cap;
        private bool closed;

        internal DepotStripe(int cap) => this.cap = cap;

        internal bool TryPush(SectorAlignedMemory page)
        {
            lock (items)
            {
                if (closed || items.Count >= cap)
                    return false;
                items.Push(page);
                return true;
            }
        }

        internal SectorAlignedMemory TryPop()
        {
            lock (items)
            {
                return items.Count > 0 ? items.Pop() : null;
            }
        }

        internal void Close(Action<SectorAlignedMemory> dropAndRelease)
        {
            lock (items)
            {
                closed = true;
                while (items.Count > 0)
                    dropAndRelease(items.Pop());
            }
        }
    }

    public sealed partial class SectorAlignedBufferPool
    {
        // ---- Size-class ladder (constrained linear-then-geometric) --------------------------------------------
        // Small linear region (exact 1..LinearSectors sectors, zero rounding waste) then 2 classes per doubling
        // (~41% worst-case internal fragmentation). Total NumClasses classes; requests above MaxPooledSectors
        // bypass the cache (allocate-on-Get, free-on-Return) so a huge read never parks a multi-MB buffer.
        private const int LinearSectors = 8;
        private const int Log2Linear = 3;               // BitOperations.Log2(LinearSectors)
        private const int GeometricDoublings = 5;
        private const int NumClasses = LinearSectors + 2 * GeometricDoublings;   // 18
        private const int MaxPooledSectors = LinearSectors << GeometricDoublings; // 256

        // Soft reuse targets (the byte budget is the only hard bound).
        private const int LocalCap = 128;               // buffers retained per (thread, class) before spilling to depot
        private const int DepotStripes = 8;             // power of two
        private const int DepotStripeCap = 1024;        // buffers per depot stripe

        // Pool lifecycle.
        private const int PoolActive = 0;
        private const int PoolClosing = 1;
        private const int PoolClosed = 2;

        /// <summary>Sentinel installed in a <see cref="Bucket.xthreadHead"/> when its owning shard retires. A
        /// claiming owner that observes it aborts (conditional CAS), and a pushing producer that observes it
        /// reroutes to the depot — the sentinel is never swapped out for null.</summary>
        internal static readonly SectorAlignedMemory Sealed = new();

        /// <summary>Per-thread map from this pool's <see cref="slotIndex"/> to the thread's <see cref="ThreadShard"/>.
        /// A recyclable-slot array (NOT ThreadLocal, NOT a monotonic index) bounded by the number of
        /// concurrently-live pools; the pool-identity check on read replaces a shard left by a pool that has
        /// since recycled the slot.</summary>
        [ThreadStatic]
        private static ThreadShard[] t_shards;

        // Process-wide free-list of released slot indices (reused after a pool is freed).
        private static readonly object s_slotLock = new();
        private static readonly Stack<int> s_freeSlots = new();
        private static int s_slotHighWater;

        // ---- Per-pool origin-return state (set in InitOriginReturn) --------------------------------------------
        private int slotIndex;
        private BudgetState budget;
        private int[] classCaps;                          // capacity in sectors per class
        private DepotStripe[] depot;                      // [NumClasses * DepotStripes]
        private List<WeakReference<ThreadShard>> registry;
        private object registryLock;
        private int poolState;                            // PoolActive / PoolClosing / PoolClosed
        private long totalManagedAllocations;             // test-only reuse-efficiency counter

        private void InitOriginReturn()
        {
            slotIndex = AcquireSlot();
            budget = new BudgetState(ManagedBudgetBytes);
            classCaps = new int[NumClasses];
            for (var c = 0; c < NumClasses; c++)
                classCaps[c] = ClassCapacitySectors(c);
            depot = new DepotStripe[NumClasses * DepotStripes];
            for (var i = 0; i < depot.Length; i++)
                depot[i] = new DepotStripe(DepotStripeCap);
            registry = new List<WeakReference<ThreadShard>>();
            registryLock = new object();
            poolState = PoolActive;
        }

        private static int AcquireSlot()
        {
            lock (s_slotLock)
                return s_freeSlots.Count > 0 ? s_freeSlots.Pop() : s_slotHighWater++;
        }

        private static void ReleaseSlot(int slot)
        {
            lock (s_slotLock)
                s_freeSlots.Push(slot);
        }

        // ---- Size-class math (pure) ----------------------------------------------------------------------------

        /// <summary>Map a sector count to a size class, or -1 to bypass the cache (request too large).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ClassOfSectors(int sectors)
        {
            if (sectors <= 0)
                sectors = 1;
            if (sectors <= LinearSectors)
                return sectors - 1;
            if (sectors > MaxPooledSectors)
                return -1;
            var octave = BitOperations.Log2((uint)(sectors - 1));   // >= Log2Linear
            var mid = 3 << (octave - 1);                            // 1.5 * 2^octave
            var sub = sectors <= mid ? 0 : 1;
            return LinearSectors + 2 * (octave - Log2Linear) + sub;
        }

        /// <summary>Capacity of a size class in sectors.</summary>
        private static int ClassCapacitySectors(int cls)
        {
            if (cls < LinearSectors)
                return cls + 1;
            var g = cls - LinearSectors;
            var octave = Log2Linear + (g >> 1);
            return (g & 1) == 0 ? (3 << (octave - 1)) : (1 << (octave + 1));
        }

        // ---- Get -----------------------------------------------------------------------------------------------

        private unsafe SectorAlignedMemory GetOriginReturn(int required_bytes, int requiredSize, bool clearOnReturn)
        {
            var sectors = sectorSizeShift >= 0 ? (requiredSize >> sectorSizeShift) : (requiredSize / sectorSize);
            var cls = ClassOfSectors(sectors);
            if (cls < 0 || Disabled)
                return AllocateUncached(required_bytes, requiredSize, clearOnReturn);

            var shard = GetOrCreateShard();
            var bucket = shard.buckets[cls];

            // 1. owner-only local stack (no atomics)
            var page = bucket.localHead;
            if (page is not null)
            {
                bucket.localHead = page.next;
                bucket.localCount--;
                bucket.localBytes -= page.permitBytes;
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);
            }

            // 2. claim the whole cross-thread chain in one seal-aware CAS, splice remainder into local
            var claimed = ClaimXThread(bucket);
            if (claimed is not null)
            {
                page = claimed;
                var rest = page.next;
                page.next = null;
                SpliceIntoLocal(bucket, rest);
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);
            }

            // 3. shared per-class depot
            page = DepotPop(cls);
            if (page is not null)
                return PrepareForRent(page, bucket, required_bytes, clearOnReturn);

            // 4. allocate (reserving a poolability permit against the byte budget)
            return AllocateForBucket(bucket, cls, required_bytes, clearOnReturn);
        }

        /// <summary>Re-arm a cached buffer for a fresh rental: re-pin (unpin mode), lazy-clear a dirty tail, set
        /// rental fields, and tag the current owner bucket.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe SectorAlignedMemory PrepareForRent(SectorAlignedMemory page, Bucket bucket, int required_bytes, bool clearOnReturn)
        {
#if CHECK_FREE
            page.Free = false;
#endif
            if (unpinOnReturn)
            {
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
                page.aligned_pointer = (byte*)RoundUp(page.handle.AddrOfPinnedObject(), sectorSize);
                page.aligned_offset = (int)((long)page.aligned_pointer - page.handle.AddrOfPinnedObject());
            }
            if (clearOnReturn && page.isDirty)
            {
                Array.Clear(page.buffer, 0, page.buffer.Length);
                page.isDirty = false;
            }
            page.required_bytes = required_bytes;
            page.clearOnReturn = clearOnReturn;
            page.originBucket = bucket;
            page.next = null;
            return page;
        }

        private unsafe SectorAlignedMemory AllocateForBucket(Bucket bucket, int cls, int required_bytes, bool clearOnReturn)
        {
            var allocBytes = checked((classCaps[cls] + 1) * sectorSize);   // extra sector for internal alignment
            var page = BuildManaged(cls, allocBytes, required_bytes, clearOnReturn);
            page.originBucket = bucket;

            // Reserve a persistent poolability permit unless the pool/shard is closing or the budget is exhausted.
            var actual = (long)page.buffer.Length;
            if (bucket.owner.state == ThreadShard.Alive && Volatile.Read(ref poolState) == PoolActive && budget.TryReserve(actual))
            {
                page.cacheable = true;
                page.permitBytes = actual;
                page.permitReleased = 0;
                page.budget = budget;
            }
            else
            {
                page.cacheable = false;
                page.permitBytes = 0;
            }
            Debug.Assert(bucket.classCapacityBytes >= RoundUp(required_bytes, sectorSize), "selected class capacity is smaller than the rounded request");
            return page;
        }

        /// <summary>Allocate a bypass buffer sized exactly to the (rounded) request; never enters the cache.</summary>
        private unsafe SectorAlignedMemory AllocateUncached(int required_bytes, int requiredSize, bool clearOnReturn)
        {
            var allocBytes = checked(requiredSize + sectorSize);
            var page = BuildManaged(0, allocBytes, required_bytes, clearOnReturn);
            page.cacheable = false;
            page.permitBytes = 0;
            page.originBucket = null;
            return page;
        }

        private unsafe SectorAlignedMemory BuildManaged(int level, int allocBytes, int required_bytes, bool clearOnReturn)
        {
            Interlocked.Increment(ref totalManagedAllocations);
            var page = new SectorAlignedMemory(level: level)
            {
                buffer = GC.AllocateArray<byte>(allocBytes, !unpinOnReturn)
            };
            if (unpinOnReturn)
                page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            var pageAddr = (long)Unsafe.AsPointer(ref page.buffer[0]);
            page.aligned_pointer = (byte*)RoundUp(pageAddr, sectorSize);
            page.aligned_offset = (int)((long)page.aligned_pointer - pageAddr);
            page.required_bytes = required_bytes;
            // Freshly-allocated buffer from GC.AllocateArray is zero-init; isDirty stays false.
            page.clearOnReturn = clearOnReturn;
            page.pool = this;
            page.next = null;
            return page;
        }

        // ---- Return --------------------------------------------------------------------------------------------

        private void ReturnOriginReturn(SectorAlignedMemory page)
        {
            if (!page.cacheable)
            {
                // Bypass / budget-overflow buffer: never cached. Drop and release any permit (none for bypass).
#if CHECK_FREE
                page.Free = true;
#endif
                DropBuffer(page);
                return;
            }

#if CHECK_FREE
            page.Free = true;
#endif
            FinalizeForReturn(page);

            var bucket = page.originBucket;
            if (bucket is null)
            {
                DropBuffer(page);
                return;
            }
            var owner = bucket.owner;

            // Same-thread owner (this pool's shard is in our slot, still alive)? -> owner-only local push, no atomics.
            // On this path the bucket is reachable via page.originBucket and owned by this live thread, so no
            // finalizer race is possible and no GC.KeepAlive is needed.
            var arr = t_shards;
            var slot = slotIndex;
            if (arr is not null && slot < arr.Length && ReferenceEquals(arr[slot], owner) && owner.state == ThreadShard.Alive)
            {
                PushLocal(bucket, page);
                return;
            }

            // Foreign Return: publish onto the origin bucket's cross-thread stack; if sealed, spill to depot;
            // if the depot is closed, drop and release the permit. KeepAlive pins the owning shard across the
            // publish so a concurrent owner-side claim + finalize cannot collect it mid-push.
            if (!TryPushXThread(bucket, page))
            {
                if (!DepotPush(page.Level, page))
                    DropBuffer(page);
            }
            GC.KeepAlive(bucket);
        }

        /// <summary>Reset rental fields and honor the clear/unpin policy BEFORE the buffer is published to any
        /// list (the publishing CAS is the release; the owner's claim is the acquire).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void FinalizeForReturn(SectorAlignedMemory page)
        {
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;
            if (page.clearOnReturn)
            {
                Array.Clear(page.buffer, 0, page.buffer.Length);
                page.isDirty = false;
            }
            else
            {
                page.isDirty = true;
            }
            page.clearOnReturn = true;
            if (unpinOnReturn)
            {
                page.handle.Free();
                page.handle = default;
                page.aligned_pointer = null;
            }
            page.next = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushLocal(Bucket bucket, SectorAlignedMemory page)
        {
            if (bucket.localCount >= LocalCap)
            {
                if (!DepotPush(page.Level, page))
                    DropBuffer(page);
                return;
            }
            page.next = bucket.localHead;
            bucket.localHead = page;
            bucket.localCount++;
            bucket.localBytes += page.permitBytes;
        }

        private void SpliceIntoLocal(Bucket bucket, SectorAlignedMemory rest)
        {
            var node = rest;
            while (node is not null && bucket.localCount < LocalCap)
            {
                var nx = node.next;
                node.next = bucket.localHead;
                bucket.localHead = node;
                bucket.localCount++;
                bucket.localBytes += node.permitBytes;
                node = nx;
            }
            while (node is not null)
            {
                var nx = node.next;
                node.next = null;
                if (!DepotPush(node.Level, node))
                    DropBuffer(node);
                node = nx;
            }
        }

        // ---- Lock-free cross-thread stack (Treiber; seal-aware) ------------------------------------------------

        /// <summary>Producer push. Fails (returns false) when the bucket is sealed so the caller reroutes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryPushXThread(Bucket bucket, SectorAlignedMemory page)
        {
            while (true)
            {
                var head = Volatile.Read(ref bucket.xthreadHead);
                if (ReferenceEquals(head, Sealed))
                    return false;
                page.next = head;
                if (ReferenceEquals(Interlocked.CompareExchange(ref bucket.xthreadHead, page, head), head))
                    return true;
            }
        }

        /// <summary>Owner claim of the whole chain via a seal-aware conditional CAS (swap head to null only when
        /// it is neither null nor the seal sentinel). This is not a CAS-pop, so there is no ABA.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SectorAlignedMemory ClaimXThread(Bucket bucket)
        {
            while (true)
            {
                var head = Volatile.Read(ref bucket.xthreadHead);
                if (head is null || ReferenceEquals(head, Sealed))
                    return null;
                if (ReferenceEquals(Interlocked.CompareExchange(ref bucket.xthreadHead, null, head), head))
                    return head;
            }
        }

        // ---- Depot ---------------------------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ThreadStripe() => Environment.CurrentManagedThreadId & (DepotStripes - 1);

        private bool DepotPush(int cls, SectorAlignedMemory page)
        {
            page.originBucket = null;    // migrating; the depot owns it now (its permit travels unchanged)
            page.next = null;
            return depot[cls * DepotStripes + ThreadStripe()].TryPush(page);
        }

        private SectorAlignedMemory DepotPop(int cls)
        {
            var baseIdx = cls * DepotStripes;
            var start = ThreadStripe();
            for (var i = 0; i < DepotStripes; i++)
            {
                var page = depot[baseIdx + ((start + i) & (DepotStripes - 1))].TryPop();
                if (page is not null)
                    return page;
            }
            return null;
        }

        // ---- Permit release / buffer drop ----------------------------------------------------------------------

        /// <summary>Release a buffer's byte-budget permit exactly once (guarded by the buffer's own flag), on its
        /// permanent drop.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReleasePermit(SectorAlignedMemory page)
        {
            var b = page.budget;
            if (b is not null && page.permitBytes != 0 && Interlocked.CompareExchange(ref page.permitReleased, 1, 0) == 0)
                b.Release(page.permitBytes);
        }

        internal static void ReleaseChainPermits(SectorAlignedMemory head)
        {
            var node = head;
            while (node is not null)
            {
                var nx = node.next;
                ReleasePermit(node);
                node = nx;
            }
        }

        /// <summary>Permanently drop a buffer: free its pin handle (unpin mode), release its permit, unroot it.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void DropBuffer(SectorAlignedMemory page)
        {
            if (unpinOnReturn && page.handle.IsAllocated)
            {
                page.handle.Free();
                page.handle = default;
            }
            ReleasePermit(page);
            page.next = null;
            page.originBucket = null;
            page.buffer = null;
            page.aligned_pointer = null;
        }

        // ---- Shard resolution ----------------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ThreadShard GetOrCreateShard()
        {
            var arr = t_shards;
            var slot = slotIndex;
            if (arr is not null && slot < arr.Length)
            {
                var s = arr[slot];
                if (s is not null && ReferenceEquals(s.pool, this))
                    return s;
            }
            return CreateShardSlow(slot);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private ThreadShard CreateShardSlow(int slot)
        {
            var arr = t_shards;
            if (arr is null || slot >= arr.Length)
            {
                var newLen = arr is null ? Math.Max(slot + 1, 4) : Math.Max(slot + 1, arr.Length * 2);
                var na = new ThreadShard[newLen];
                if (arr is not null)
                    Array.Copy(arr, na, arr.Length);
                t_shards = na;
                arr = na;
            }

            ThreadShard shard;
            lock (registryLock)
            {
                var bornSealed = poolState != PoolActive;
                shard = new ThreadShard(this, NumClasses, budget, sectorSize, classCaps, bornSealed);
                if (bornSealed)
                    shard.drainedOnce = 1;      // nothing cached; no drain, no permits held
                else
                    registry.Add(new WeakReference<ThreadShard>(shard));
            }
            arr[slot] = shard;
            return shard;
        }

        // ---- Teardown ------------------------------------------------------------------------------------------

        private void FreeOriginReturn()
        {
            List<ThreadShard> live;
            lock (registryLock)
            {
                if (poolState == PoolClosed)
                    return;
                poolState = PoolClosing;    // stops new permit reservations and makes new shards born-sealed
                live = new List<ThreadShard>(registry.Count);
                foreach (var wr in registry)
                    if (wr.TryGetTarget(out var s))
                        live.Add(s);
                registry.Clear();
            }

            foreach (var shard in live)
                SealAndDrainShard(shard);

            if (depot is not null)
                foreach (var stripe in depot)
                    stripe.Close(DropBuffer);

            lock (registryLock)
                poolState = PoolClosed;

            ReleaseSlot(slotIndex);
        }

        private void SealAndDrainShard(ThreadShard shard)
        {
            if (Interlocked.CompareExchange(ref shard.drainedOnce, 1, 0) != 0)
                return;    // finalizer (or a prior drain) already handled it
            Interlocked.Exchange(ref shard.state, ThreadShard.Sealed);

            var buckets = shard.buckets;
            if (buckets is not null)
            {
                foreach (var bucket in buckets)
                {
                    if (bucket is null)
                        continue;
                    var chain = Interlocked.Exchange(ref bucket.xthreadHead, Sealed);
                    if (!ReferenceEquals(chain, Sealed))
                        DropChain(chain);
                    DropChain(bucket.localHead);
                    bucket.localHead = null;
                    bucket.localCount = 0;
                    bucket.localBytes = 0;
                }
            }

            // Tombstone: keep each bucket's xthreadHead == Sealed (late foreign Returns must still reroute), but
            // drop the shard's heavy references so a lingering thread-static slot roots nothing large.
            shard.buckets = null;
            shard.pool = null;
        }

        private void DropChain(SectorAlignedMemory head)
        {
            var node = head;
            while (node is not null)
            {
                var nx = node.next;
                node.next = null;
                DropBuffer(node);
                node = nx;
            }
        }

        private void PrintOriginReturn()
        {
            List<ThreadShard> live;
            lock (registryLock)
            {
                live = new List<ThreadShard>(registry.Count);
                foreach (var wr in registry)
                    if (wr.TryGetTarget(out var s))
                        live.Add(s);
            }
            Console.WriteLine($"  origin-return pool: {live.Count} live shard(s), budget {budget.Used}/{budget.budgetBytes} bytes reserved");
        }

        // ---- Diagnostics (used by tests) -----------------------------------------------------------------------

        /// <summary>Bytes currently reserved against the pool's budget (0 at full quiesce). Test-only.</summary>
        internal long ReservedBytes => budget?.Used ?? 0;
        /// <summary>Total managed buffer allocations served by this pool (reuse-efficiency measure). Test-only.</summary>
        internal long TotalManagedAllocations => Interlocked.Read(ref totalManagedAllocations);
        /// <summary>Number of live shards registered with this pool. Test-only.</summary>
        internal int LiveShardCount
        {
            get
            {
                if (registry is null)
                    return 0;
                var n = 0;
                lock (registryLock)
                    foreach (var wr in registry)
                        if (wr.TryGetTarget(out _))
                            n++;
                return n;
            }
        }

        /// <summary>Current length of the calling thread's shard-slot array. Test-only (bounded-growth check).</summary>
        internal static int ThreadShardArrayLength => t_shards?.Length ?? 0;

        // ---- Size-class ladder test hooks ----------------------------------------------------------------------
        internal static int TestClassOfSectors(int sectors) => ClassOfSectors(sectors);
        internal static int TestClassCapacitySectors(int cls) => ClassCapacitySectors(cls);
        internal static int TestNumClasses => NumClasses;
        internal static int TestMaxPooledSectors => MaxPooledSectors;
        internal static int TestLinearSectors => LinearSectors;

        // ---- Native wrapper pool (bounded, lightly-striped, pool-agnostic) -------------------------------------
        // Replaces the old [ThreadStatic] wrapper free list, whose Get-on-issuing / Return-on-completion asymmetry
        // stranded wrappers on completion threads. Wrappers are tiny and pool-agnostic, so a simple global striped
        // stack suffices (full origin-return is overkill for them).
        private const int WrapperStripes = 16;          // power of two
        private const int WrapperStripeCap = 256;
        private static readonly ConcurrentStack<SectorAlignedMemory>[] s_wrapperPool = CreateWrapperPool();
        private static readonly int[] s_wrapperCount = new int[WrapperStripes];

        private static ConcurrentStack<SectorAlignedMemory>[] CreateWrapperPool()
        {
            var pool = new ConcurrentStack<SectorAlignedMemory>[WrapperStripes];
            for (var i = 0; i < WrapperStripes; i++)
                pool[i] = new ConcurrentStack<SectorAlignedMemory>();
            return pool;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SectorAlignedMemory RentWrapperGlobal()
        {
            var idx = Environment.CurrentManagedThreadId & (WrapperStripes - 1);
            if (s_wrapperPool[idx].TryPop(out var w))
            {
                Interlocked.Decrement(ref s_wrapperCount[idx]);
#if CHECK_FREE
                w.Free = false;
#endif
                return w;
            }
            return new SectorAlignedMemory();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ReturnWrapperGlobal(SectorAlignedMemory page)
        {
            var idx = Environment.CurrentManagedThreadId & (WrapperStripes - 1);
            if (Volatile.Read(ref s_wrapperCount[idx]) >= WrapperStripeCap)
                return;    // soft cap reached; let Gen0 reclaim the wrapper
            Interlocked.Increment(ref s_wrapperCount[idx]);
            s_wrapperPool[idx].Push(page);
        }
    }
}