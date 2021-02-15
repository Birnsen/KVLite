
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class IteratorTests
    {
        private KeyValuePair<byte[], byte[]> createKVP(int keyId, string value)
        {
            var key = Encoding.UTF8.GetBytes($"key{keyId}");
            var val = Encoding.UTF8.GetBytes(value);

            return new KeyValuePair<byte[], byte[]>(key, val);
        }

        [TestMethod]
        public async Task TestGetAll()
        {
            var kvl = KVLite.CreateInMemory();
            var entryCount = 1024000;

            var entries = Enumerable
                .Range(0, entryCount)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);
            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var counter = 0;
            await foreach (var res in kvl.Get(true))
            {
                Interlocked.Increment(ref counter);
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);
                Assert.AreEqual("value", Encoding.UTF8.GetString(res.Value));
            }

            Assert.AreEqual(0, keys.Count);
            Assert.AreEqual(counter, entryCount);
        }

        [TestMethod]
        public async Task TestGetAllPagination()
        {
            using var kvl = KVLite.CreateInMemory();
            var entryCount = 1024;

            var entries = Enumerable
                .Range(0, entryCount)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);
            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            await foreach (var k in kvl.Get().BatchAsync(10).Take(2))
            {
                await foreach (var res in k)
                {
                    var key = Encoding.UTF8.GetString(res.Key);
                    keys.Remove(key);
                    Assert.AreEqual("value", Encoding.UTF8.GetString(res.Value));
                }
            }

            Assert.AreEqual(entryCount - 20, keys.Count);
        }

        [DataTestMethod]
        /*[DataRow(128, false)]
        [DataRow(128, true)]
        [DataRow(1024, false)]
        [DataRow(1024, true)]
        [DataRow(10240, false)]
        [DataRow(10240, true)]//*/
        [DataRow(61504, false)]
        [DataRow(1024000, true)]
        public async Task TestGetByComparison(int entryCount, bool random)
        {
            var kvl = KVLite.CreateJsonInMemory();
            var valueTrue = JsonSerializer.Serialize(new { done = true });
            var valueFalse = JsonSerializer.Serialize(new { done = false });
            var rnd = new Random(Guid.NewGuid().GetHashCode());

            var entries = Enumerable
                .Range(0, entryCount)
                .Select(x =>
                {
                    var value = random
                        ? rnd.Next(0, 100) > 50
                            ? valueTrue : valueFalse
                        : x < entryCount / 2
                            ? valueTrue : valueFalse;
                    var key = Encoding.UTF8.GetBytes($"key{x}");

                    return new KeyValuePair<byte[], string>(key, value);
                }).ToList();

            await kvl.Add(entries);
            var groups = entries.GroupBy(x => x.Value.Contains("true"));
            var trueKeys = groups
                .Where(x => x.Key)
                .First()
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var falseKeys = groups
                .Where(x => !x.Key)
                .First()
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var trueCount = trueKeys.Count;
            var falseCount = falseKeys.Count;
            var counter = 0;
            var hs = new HashSet<string>();
            await foreach (var res in kvl.Get<string, bool>("$.done", Compare.EQ, true))
            {
                Interlocked.Increment(ref counter);
                var key = Encoding.UTF8.GetString(res.Key);
                trueKeys.Remove(key);
                hs.Add(key);

                Assert.AreEqual(valueTrue, res.Value);
            }

            await foreach (var res in kvl.Get<string, bool>("$.done", Compare.NE, true))
            {
                Interlocked.Increment(ref counter);
                var key = Encoding.UTF8.GetString(res.Key);
                falseKeys.Remove(key);
                hs.Add(key);

                Assert.AreEqual(valueFalse, res.Value);
            }

            Assert.AreEqual(hs.Count, entryCount);
            Assert.AreEqual(0, trueKeys.Count);
            Assert.AreEqual(counter, entryCount);
        }

        [DataTestMethod]
        /*[DataRow(128, false)]
        [DataRow(128, true)]
        [DataRow(1024, false)]
        [DataRow(1024, true)]
        [DataRow(10240, false)]
        [DataRow(10240, true)]//*/
        [DataRow(61504, false)]
        [DataRow(1024000, true)]
        public async Task TestGetRRByComparison(int entryCount, bool random)
        {
            var kvl = KVLite.CreateJsonInMemory();
            var valueTrue = JsonSerializer.Serialize(new { done = true });
            var valueFalse = JsonSerializer.Serialize(new { done = false });
            var rnd = new Random(Guid.NewGuid().GetHashCode());

            var entries = Enumerable
                .Range(0, entryCount)
                .Select(x =>
                {
                    var value = random
                        ? rnd.Next(0, 100) > 50
                            ? valueTrue : valueFalse
                        : x < entryCount / 2
                            ? valueTrue : valueFalse;
                    var key = Encoding.UTF8.GetBytes($"key{x}");

                    return new KeyValuePair<byte[], string>(key, value);
                }).ToList();

            await kvl.Add(entries);
            var groups = entries.GroupBy(x => x.Value.Contains("true"));
            var trueKeys = groups
                .Where(x => x.Key)
                .First()
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var falseKeys = groups
                .Where(x => !x.Key)
                .First()
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var trueCount = trueKeys.Count;
            var falseCount = falseKeys.Count;
            var counter = 0;
            var hs = new HashSet<string>();
            await foreach (var res in kvl.GetRR<string, bool>("$.done", Compare.EQ, true))
            {
                Interlocked.Increment(ref counter);
                var key = Encoding.UTF8.GetString(res.Key);
                trueKeys.Remove(key);
                hs.Add(key);

                Assert.AreEqual(valueTrue, res.Value);
            }

            await foreach (var res in kvl.GetRR<string, bool>("$.done", Compare.NE, true))
            {
                Interlocked.Increment(ref counter);
                var key = Encoding.UTF8.GetString(res.Key);
                falseKeys.Remove(key);
                hs.Add(key);

                Assert.AreEqual(valueFalse, res.Value);
            }

            Assert.AreEqual(0, trueKeys.Count);
            Assert.AreEqual(counter, entryCount);
            Assert.AreEqual(hs.Count, entryCount);
        }
    }
}