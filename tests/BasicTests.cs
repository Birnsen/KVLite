using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KVL.Tests
{
    [TestClass]
    public class BasicTests
    {
        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            await kvl.Add(key, value);
            var res = await kvl.Get(key);

            Assert.IsTrue(res.IsSome);
            Assert.AreEqual("value", Encoding.UTF8.GetString(res.Head()));
        }

        [TestMethod]
        public async Task TestAddSameKeyTwice()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            await kvl.Add(key, value);
            await kvl.Add(key, value);
            var res = await kvl.Get(key);

            Assert.IsTrue(res.IsSome);
            Assert.AreEqual("value", Encoding.UTF8.GetString(res.Head()));
        }

        [TestMethod]
        public async Task TestAddSameKeyTwiceWithDifferentValue()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value1 = Encoding.UTF8.GetBytes("value1");
            var value2 = Encoding.UTF8.GetBytes("value2");

            await kvl.Add(key, value1);
            await kvl.Add(key, value2);
            var res = await kvl.Get(key);

            Assert.IsTrue(res.IsSome);
            Assert.AreEqual("value1", Encoding.UTF8.GetString(res.Head()));
        }

        [TestMethod]
        public async Task TestGetNonExisting()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");
            var newValue = Encoding.UTF8.GetBytes("newValue");

            await kvl.Add(key, value);
            await kvl.Update(key, newValue);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsSome);
            Assert.AreEqual("newValue", Encoding.UTF8.GetString(res.Head()));
        }

        [TestMethod]
        public async Task TestUpdateTwice()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");
            var newValue = Encoding.UTF8.GetBytes("newValue");

            await kvl.Add(key, value);
            await kvl.Update(key, newValue);
            await kvl.Update(key, newValue);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsSome);
            Assert.AreEqual("newValue", Encoding.UTF8.GetString(res.Head()));
        }

        [TestMethod]
        public async Task TestUpdateNonExisting()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");
            var newValue = Encoding.UTF8.GetBytes("newValue");

            await kvl.Update(key, newValue);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            await kvl.Add(key, value);
            await kvl.Delete(key);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestDeleteNonExisting()
        {
            var kvl = KVLite.CreateInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            await kvl.Delete(key);
        }

        [TestMethod]
        public async Task TestCount()
        {
            var kvl = KVLite.CreateInMemory();
            var value = Encoding.UTF8.GetBytes("value");

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i}");
                await kvl.Add(key, value);
            }

            var count = await kvl.Count();

            Assert.AreEqual(1000, count);
        }

    }
}
