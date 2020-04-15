
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
            var entryCount = 1024;

            var entries = Enumerable
                .Range(0, entryCount)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);
            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();

            var counter = 0;
            await foreach(var res in kvl.Get())
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

            await foreach(var k in kvl.Get().Batch(10).Take(2))
            {
                await foreach(var res in k)
                {
                    var key = Encoding.UTF8.GetString(res.Key);
                    keys.Remove(key);
                    Assert.AreEqual("value", Encoding.UTF8.GetString(res.Value));
                }
            }

            Assert.AreEqual(entryCount - 20, keys.Count);
        }
    }
}