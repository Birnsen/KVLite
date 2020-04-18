using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;

namespace KVL.Tests
{
    [TestClass]
    public class BasicJsonTests
    {
        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");

            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            var res = await kvl.Get(key);

            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestAddSameKeyTwice()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Add(key, value);
            var res = await kvl.Get(key);

            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestAddSameKeyTwiceWithDifferentValue()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value1 = JsonSerializer.Serialize(new {hello = "value1"});
            var value2 = JsonSerializer.Serialize(new {hello = "value2"});

            await kvl.Add(key, value1);
            await kvl.Add(key, value2);
            var res = await kvl.Get(key);

            Assert.AreEqual(value1, res);
        }

        [TestMethod]
        public async Task TestGetNonExisting()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestUpdate()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var newValue = JsonSerializer.Serialize(new {hello = "newValue"});

            await kvl.Add(key, value);
            await kvl.Update(key, newValue);

            var res = await kvl.Get(key);
            Assert.AreEqual(newValue, res);
        }

        [TestMethod]
        public async Task TestUpdateTwice()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var newValue = JsonSerializer.Serialize(new {hello = "newValue"});

            await kvl.Add(key, value);
            await kvl.Update(key, newValue);
            await kvl.Update(key, newValue);

            var res = await kvl.Get(key);
            Assert.AreEqual(newValue, res);
        }

        [TestMethod]
        public async Task TestUpdateNonExisting()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Update(key, value);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestDelete()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Delete(key);

            var res = await kvl.Get(key);
            Assert.IsTrue(res.IsNone);
        }

        [TestMethod]
        public async Task TestDeleteNonExisting()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Delete(key);
        }

        [TestMethod]
        public async Task TestInsert()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Insert(key, "$.world", "value");
            var res = await kvl.Get(key);

            var expected = JsonSerializer.Serialize(new {hello = "value", world = "value"});
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestInsertDoesNotOverwrite()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Insert(key, "$.hello", "newValue");
            var res = await kvl.Get(key);

            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestInsertIntoArray()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = new [] {1,2,3}});

            await kvl.Add(key, value);
            await kvl.Insert(key, "$.hello[#]", 4);
            var res = await kvl.Get(key);

            var expected = JsonSerializer.Serialize(new {hello = new [] {1,2,3,4}});
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestReplace()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Replace(key, "$.hello", "newValue");
            var res = await kvl.Get(key);

            var expected = JsonSerializer.Serialize(new {hello = "newValue"});
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestReplaceDoesNotCreate()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Replace(key, "$.world", "value");
            var res = await kvl.Get(key);

            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestSetDoesOverwrite()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Set(key, "$.hello", "newValue");
            var res = await kvl.Get(key);

            var expected = JsonSerializer.Serialize(new {hello = "newValue"});
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestSetDoesCreate()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);
            await kvl.Set(key, "$.world", "value");
            var res = await kvl.Get(key);

            var expected = JsonSerializer.Serialize(new {hello = "value", world = "value"});
            Assert.AreEqual(expected, res);
        }
    }
}
