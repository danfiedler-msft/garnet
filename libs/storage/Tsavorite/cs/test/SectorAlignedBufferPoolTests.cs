// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Correctness tests for the default origin-return managed <see cref="SectorAlignedBufferPool"/>: cross-thread
    /// (mimalloc <c>xthread_free</c>-style) return routing, size-class ladder arithmetic, per-pool byte budget /
    /// poolability permits, retirement/seal + <see cref="SectorAlignedBufferPool.Free"/> teardown, dead-thread
    /// permit reclamation, and multi-pool isolation. These run in the normal (non-<c>[Explicit]</c>) suite.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public unsafe class SectorAlignedBufferPoolTests
    {
        const int SectorSize = 512;

        [SetUp]
        public void Setup()
        {
            // Ensure the origin-return default and no native backend for these fixtures.
            SectorAlignedBufferPool.NativeAllocator = null;
            SectorAlignedBufferPool.Disabled = false;
            SectorAlignedBufferPool.UnpinOnReturn = false;
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        [TearDown]
        public void TearDown()
        {
            SectorAlignedBufferPool.NativeAllocator = null;
            SectorAlignedBufferPool.Disabled = false;
            SectorAlignedBufferPool.UnpinOnReturn = false;
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        // ---- Size-class ladder ---------------------------------------------------------------------------------

        [Test]
        public void LadderIsMonotonicAndBounded()
        {
            var maxSectors = SectorAlignedBufferPool.TestMaxPooledSectors;
            var numClasses = SectorAlignedBufferPool.TestNumClasses;
            var linear = SectorAlignedBufferPool.TestLinearSectors;

            var prevClass = -1;
            for (var s = 1; s <= maxSectors; s++)
            {
                var cls = SectorAlignedBufferPool.TestClassOfSectors(s);
                ClassicAssert.GreaterOrEqual(cls, 0, $"sectors={s} within cap must have a class");
                ClassicAssert.Less(cls, numClasses, $"sectors={s} class in range");
                ClassicAssert.GreaterOrEqual(cls, prevClass, $"class must be monotonic non-decreasing at sectors={s}");
                prevClass = cls;

                var cap = SectorAlignedBufferPool.TestClassCapacitySectors(cls);
                ClassicAssert.GreaterOrEqual(cap, s, $"class capacity {cap} must cover request {s} sectors");

                // Linear region is exact (no rounding waste); geometric region bounded ~2x.
                if (s <= linear)
                    ClassicAssert.AreEqual(s, cap, "linear region must be exact");
                else
                    ClassicAssert.LessOrEqual(cap, 2 * s, "geometric fragmentation must be bounded < 2x");
            }

            // Just above the cap => bypass.
            ClassicAssert.AreEqual(-1, SectorAlignedBufferPool.TestClassOfSectors(maxSectors + 1), "over-cap request must bypass");
        }

        [Test]
        public void GetCapacityCoversRequestAcrossSizes()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                foreach (var bytes in new[] { 1, 100, 511, 512, 513, 4096, 4097, 60000, 200000, 2_000_000 })
                {
                    var page = pool.Get(bytes, clearOnReturn: false);
                    try
                    {
                        ClassicAssert.AreEqual(0, ((long)page.aligned_pointer) % SectorSize, "aligned");
                        ClassicAssert.GreaterOrEqual(page.AlignedTotalCapacity, bytes, $"capacity covers {bytes}");
                        // Touch first + last usable byte.
                        page.aligned_pointer[0] = 1;
                        page.aligned_pointer[bytes - 1] = 1;
                    }
                    finally { page.Return(); }
                }
            }
            finally { pool.Free(); }
        }

        // ---- Get/Return basics ---------------------------------------------------------------------------------

        [Test]
        public void GetReturnsZeroedBufferAndReuses()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096);
                for (var i = 0; i < p1.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p1.aligned_pointer[i], "default Get must be zeroed");
                for (var i = 0; i < 4096; i++)
                    p1.aligned_pointer[i] = 0xAB;
                p1.Return();

                // Same thread => local reuse => same object handed back, re-zeroed by default policy.
                var p2 = pool.Get(4096);
                ClassicAssert.AreSame(p1, p2, "same-thread Return then Get should reuse the local buffer");
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "reused buffer must be re-zeroed");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void OptOutClearThenDefaultGetIsZeroed()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096, clearOnReturn: false);
                for (var i = 0; i < 4096; i++)
                    p1.aligned_pointer[i] = 0xCD;
                p1.Return();    // opted out => dirty, not cleared on Return

                var p2 = pool.Get(4096);    // default clearOnReturn:true => lazy-clear the dirty tail
                ClassicAssert.AreSame(p1, p2);
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "default Get must lazy-clear a dirty slot");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        // ---- Cross-thread (origin) return routing --------------------------------------------------------------

        [Test]
        public void CrossThreadReturnRoutesBackToOriginAndReuses()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // Get on this (origin) thread; Return on a different thread. The buffer must route back to the
                // origin thread's shard so a subsequent Get here reuses it (rather than allocating fresh).
                var p1 = pool.Get(4096, clearOnReturn: false);
                var ptr1 = (long)p1.aligned_pointer;

                var t = new Thread(() => p1.Return());
                t.Start();
                t.Join();

                var p2 = pool.Get(4096, clearOnReturn: false);
                ClassicAssert.AreSame(p1, p2, "cross-thread Return must route back to the origin thread and be reused");
                ClassicAssert.AreEqual(ptr1, (long)p2.aligned_pointer, "reused buffer retains its allocation");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void CompletionOnlyThreadCreatesNoShard()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var page = pool.Get(4096, clearOnReturn: false);

                int slotArrayLenOnConsumer = -1;
                var t = new Thread(() =>
                {
                    // This thread only ever Returns; it must not create a shard/slot-array for the pool.
                    page.Return();
                    slotArrayLenOnConsumer = SectorAlignedBufferPool.ThreadShardArrayLength;
                });
                t.Start();
                t.Join();

                ClassicAssert.AreEqual(0, slotArrayLenOnConsumer, "a completion-only thread must not allocate a shard slot array");
            }
            finally { pool.Free(); }
        }

        // ---- Multi-pool isolation ------------------------------------------------------------------------------

        [Test]
        public void MultiplePoolsDifferentSectorSizesNoCorruption()
        {
            var poolA = new SectorAlignedBufferPool(1, 512);
            var poolB = new SectorAlignedBufferPool(1, 4096);
            try
            {
                // Same nominal class index maps to different byte capacities per pool; interleave to ensure a
                // buffer from one pool is never handed out by the other.
                for (var iter = 0; iter < 1000; iter++)
                {
                    var a = poolA.Get(2000, clearOnReturn: false);
                    var b = poolB.Get(2000, clearOnReturn: false);
                    ClassicAssert.AreEqual(0, ((long)a.aligned_pointer) % 512);
                    ClassicAssert.AreEqual(0, ((long)b.aligned_pointer) % 4096);
                    ClassicAssert.GreaterOrEqual(a.AlignedTotalCapacity, 2000);
                    ClassicAssert.GreaterOrEqual(b.AlignedTotalCapacity, 2000);
                    a.aligned_pointer[1999] = 1;
                    b.aligned_pointer[1999] = 1;
                    a.Return();
                    b.Return();
                }
            }
            finally
            {
                poolA.Free();
                poolB.Free();
            }
        }

        // ---- Byte budget / permits -----------------------------------------------------------------------------

        [Test]
        public void BudgetBoundsReusableBytesAndReturnsToZero()
        {
            // Tiny budget: only a handful of 4 KB-class buffers may be cached; the rest are served non-cacheable
            // and dropped on Return. The reserved counter must never exceed the budget and must reach 0 at quiesce.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64 * 1024;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var held = new List<SectorAlignedMemory>();
                for (var i = 0; i < 256; i++)
                    held.Add(pool.Get(4096, clearOnReturn: false));

                ClassicAssert.LessOrEqual(pool.ReservedBytes, SectorAlignedBufferPool.ManagedBudgetBytes,
                    "reserved bytes must never exceed the budget");

                foreach (var p in held)
                    p.Return();

                pool.Free();
                ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
            }
            finally { pool.Free(); }
        }

        [Test]
        public void FreeAfterCrossThreadReturnsIsCleanAndBudgetZero()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var barrier = new Barrier(9);
            var stop = false;
            var tasks = new List<Task>();

            // 4 producers Get, hand to a shared channel; 4 consumers Return cross-thread. Free() then races them.
            var channel = new ConcurrentQueue<SectorAlignedMemory>();
            for (var i = 0; i < 4; i++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    barrier.SignalAndWait();
                    while (!Volatile.Read(ref stop))
                    {
                        var p = pool.Get(4096, clearOnReturn: false);
                        channel.Enqueue(p);
                    }
                }, TaskCreationOptions.LongRunning));
            }
            for (var i = 0; i < 4; i++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    barrier.SignalAndWait();
                    while (!Volatile.Read(ref stop) || !channel.IsEmpty)
                    {
                        if (channel.TryDequeue(out var p))
                            p.Return();
                    }
                }, TaskCreationOptions.LongRunning));
            }

            barrier.SignalAndWait();
            Thread.Sleep(300);
            Volatile.Write(ref stop, true);
            Task.WaitAll(tasks.ToArray());

            // Drain any stragglers then Free and assert budget quiesces to zero.
            while (channel.TryDequeue(out var p))
                p.Return();
            pool.Free();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must quiesce to zero after concurrent Free");
        }

        // ---- Dead-thread permit reclamation --------------------------------------------------------------------

        [Test]
        public void DeadThreadFinalizerReclaimsPermits()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // A worker thread Gets and same-thread Returns (caching buffers in its local list), then exits
                // WITHOUT the pool being freed. Its shard becomes unreachable; the finalizer must release the
                // held permits back to the budget.
                for (var t = 0; t < 4; t++)
                {
                    var th = new Thread(() =>
                    {
                        for (var i = 0; i < 32; i++)
                        {
                            var p = pool.Get(4096, clearOnReturn: false);
                            p.Return();     // same-thread => cached locally, holds a permit
                        }
                    });
                    th.Start();
                    th.Join();
                }

                ClassicAssert.Greater(pool.ReservedBytes, 0, "cached buffers on live-but-dead-thread shards hold permits");

                for (var attempt = 0; attempt < 10 && pool.ReservedBytes > 0; attempt++)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Thread.Sleep(50);
                }
                ClassicAssert.AreEqual(0, pool.ReservedBytes, "dead-thread shard finalizers must reclaim all permits");
            }
            finally { pool.Free(); }
        }

        // ---- Double-return guard (CHECK_FREE / DEBUG) ----------------------------------------------------------

        [Test]
        public void DoubleReturnIsCaughtInDebug()
        {
#if !DEBUG
            Assert.Ignore("double-return guard (CHECK_FREE) only compiled in DEBUG");
#endif
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p = pool.Get(4096, clearOnReturn: false);
                p.Return();
                ClassicAssert.Throws<TsavoriteException>(() => p.Return(), "a double Return must be caught under CHECK_FREE");
            }
            finally { pool.Free(); }
        }

        // ---- Kill-switch (legacy path still works) -------------------------------------------------------------

        [Test]
        public void LegacyPathStillWorksWhenOriginReturnDisabled()
        {
            SectorAlignedBufferPool.UseOriginReturn = false;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096);
                for (var i = 0; i < p1.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p1.aligned_pointer[i]);
                p1.Return();
                var p2 = pool.Get(4096);
                ClassicAssert.AreEqual(0, ((long)p2.aligned_pointer) % SectorSize);
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void FreeIsIdempotent()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var p = pool.Get(4096, clearOnReturn: false);
            p.Return();
            pool.Free();
            Assert.DoesNotThrow(() => pool.Free(), "Free must be idempotent");
        }
    }
}