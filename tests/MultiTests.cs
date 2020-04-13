using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class MultiTests
    {
        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateInMemory();
            var value = Encoding.UTF8.GetBytes("value");

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Add(key, value);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                var res = await kvl.Get(key);
                Assert.AreEqual("value", Encoding.UTF8.GetString(res));
            }
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateInMemory();
            var value = Encoding.UTF8.GetBytes("value");
            var newValue = Encoding.UTF8.GetBytes("newValue");

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
                Assert.AreEqual("newValue", Encoding.UTF8.GetString(res));
            }
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateInMemory();
            var value = Encoding.UTF8.GetBytes("value");

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
