
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class BulkTests
    {
        private KeyValuePair<byte[], byte[]> createKVP(int keyId, string value)
        {
            var key = Encoding.UTF8.GetBytes($"key{keyId}");
            var val = Encoding.UTF8.GetBytes(value);

            return new KeyValuePair<byte[], byte[]>(key, val);
        }

        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateInMemory();

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);

            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();
            await foreach(var res in kvl.Get())
            {
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);

                Assert.AreEqual("value", Encoding.UTF8.GetString(res.Value));
            }

            Assert.AreEqual(0, keys.Count);
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateInMemory();

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);

            var newEntries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, "newValue"));

            await kvl.Update(newEntries);

            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();
            await foreach(var res in kvl.Get())
            {
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);
                Assert.AreEqual("newValue", Encoding.UTF8.GetString(res.Value));
            }

            Assert.AreEqual(0, keys.Count);
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateInMemory();

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);

            var keys = entries.Select(x => x.Key);

            await kvl.Delete(keys);

            foreach(var key in keys)
            {
                var res = await kvl.Get(key);
                Assert.IsTrue(res.IsNone);
            }

        }

        [TestMethod]
        public async Task TestGetRR()
        {
            var kvl = KVLite.CreateInMemory();

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, "value"));

            await kvl.Add(entries);

            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();
            var counter = 0;
            await foreach (var res in kvl.GetRR())
            {
                counter++;
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);

                Assert.AreEqual("value", Encoding.UTF8.GetString(res.Value));
            }

            Assert.AreEqual(1000, counter);
            Assert.AreEqual(0, keys.Count);
        }
    }
}