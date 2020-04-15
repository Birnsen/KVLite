using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class MultiJsonTests
    {
        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Add(key, value);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                var res = await kvl.Get(key);
                Assert.AreEqual(value, res);
            }
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var newValue = JsonSerializer.Serialize(new {hello = "newValue"});

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Add(key, value);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Update(key, newValue);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                var res = await kvl.Get(key);
                Assert.AreEqual(newValue, res);
            }
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var value = JsonSerializer.Serialize(new {hello = "value"});

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Add(key, value);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Delete(key);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await Assert.ThrowsExceptionAsync<Exception>(async () => await kvl.Get(key), "Key not found!");
            }
        }

    }
}
