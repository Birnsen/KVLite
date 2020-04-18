
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class BulkJsonTests
    {
        private KeyValuePair<byte[], string> createKVP(int keyId, string value)
        {
            var key = Encoding.UTF8.GetBytes($"key{keyId}");

            return new KeyValuePair<byte[], string>(key, value);
        }

        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, value));

            await kvl.Add(entries);

            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();
            await foreach(var res in kvl.Get())
            {
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);

                Assert.AreEqual(value, res.Value);
            }

            Assert.AreEqual(0, keys.Count);
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var newValue = JsonSerializer.Serialize(new {hello = "newValue"});

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, value));

            await kvl.Add(entries);

            var newEntries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, newValue));

            await kvl.Update(newEntries);

            var keys = entries
                .Select(x => Encoding.UTF8.GetString(x.Key))
                .ToHashSet();
            await foreach(var res in kvl.Get())
            {
                var key = Encoding.UTF8.GetString(res.Key);
                keys.Remove(key);
                Assert.AreEqual(newValue, res.Value);
            }

            Assert.AreEqual(0, keys.Count);
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});

            var entries = Enumerable
                .Range(0, 1000)
                .Select(x => createKVP(x, value));

            await kvl.Add(entries);

            var keys = entries.Select(x => x.Key);
            await kvl.Delete(keys);

            foreach(var key in keys)
            {
                var res = await kvl.Get(key);
                Assert.IsTrue(res.IsNone);
            }

        }
    }
}