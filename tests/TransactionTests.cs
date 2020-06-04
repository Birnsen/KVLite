
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;
using System.Linq;

namespace KVL.Tests
{
    [TestClass]
    public class TransactionTests
    {
        [TestMethod]
        public async Task TestCommitTransaction()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var value2 = JsonSerializer.Serialize("value");

            using var trx = await kvl.BeginTransactionAsync(key);
            await kvl.Add(key, value);
            await kvl.Insert(key, "$.world", value2);
            await trx.CommitAsync();

            var res = await kvl.Get(key);
            var expected = JsonSerializer.Serialize(new {hello = "value", world = "value"});
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestRollbackTransaction()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var value2 = JsonSerializer.Serialize("value");

            await kvl.Add(key, value);

            using var trx = await kvl.BeginTransactionAsync(key);
            await kvl.Insert(key, "$.world", value2);
            await trx.RollbackAsync();

            var res = await kvl.Get(key);
            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestMulitpleInsertsPerTransaction()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            using var trx = await kvl.BeginTransactionAsync(key);
            await kvl.Add(key, value);
            await kvl.Set(key, "$.world", "[0]");
            await kvl.Insert(key, "$.world[#]", 1);
            await kvl.Set(key, "$.world[#]", 2);

            await trx.CommitAsync();
            
            var expected = JsonSerializer.Serialize(new {hello = "value", world = new [] {0,1,2}});
            var res = await kvl.Get(key);
            Assert.AreEqual(expected, res);
        }

        [TestMethod]
        public async Task TestDisposeRollsBack()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});

            await kvl.Add(key, value);

            using (var trx = await kvl.BeginTransactionAsync(key))
            {
                await kvl.Insert(key, "$.world", "[0]");
                await kvl.Insert(key, "$.world[#]", 1);
                await kvl.Insert(key, "$.world[#]", 2);
            }
            
            var res = await kvl.Get(key);
            Assert.AreEqual(value, res);
        }

        [TestMethod]
        public async Task TestMultiThreadedTransactionOnSameKey()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var arrayValues = Enumerable.Range(0, 100);

            await kvl.Add(key, value);

            var tasks = arrayValues
                .Select(trxFunc);

            await Task.WhenAll(tasks);
            
            var res = await kvl.Get(key);
            var element = JsonDocument.Parse(res.Head()).RootElement;
            var prop = element
                .EnumerateObject()
                .Find(x => x.Name == "world").Head();
            var values = arrayValues.ToHashSet();
            foreach(var i in prop.Value.EnumerateArray())
            {
                values.Remove(i.GetInt32());
            }

            Assert.AreEqual(0, values.Count);

            async Task trxFunc(int i)
            {
                using var trx = await kvl.BeginTransactionAsync(key);
                await kvl.Insert(key, "$.world", "[]");
                await kvl.Insert(key, "$.world[#]", i);
                await trx.CommitAsync();
            }
        }

        [TestMethod]
        public async Task TestMultiThreadedTransactionOnSameKeyOneThrows()
        {
            var kvl = KVLite.CreateJsonInMemory();
            var key = Encoding.UTF8.GetBytes("key");
            var value = JsonSerializer.Serialize(new {hello = "value"});
            var arrayValues = Enumerable.Range(0, 100);

            await kvl.Add(key, value);

            var tasks = arrayValues
                .Select(trxFunc);

            var t = await Assert.ThrowsExceptionAsync<Exception>(() => Task.WhenAll(tasks));
            
            var res = await kvl.Get(key);
            var element = JsonDocument.Parse(res.Head()).RootElement;
            var prop = element
                .EnumerateObject()
                .Find(x => x.Name == "world").Head();
            var values = arrayValues.ToHashSet();
            foreach(var i in prop.Value.EnumerateArray())
            {
                values.Remove(i.GetInt32());
            }

            Assert.AreEqual(1, values.Count);

            async Task trxFunc(int i)
            {
                if (i == 50) throw new Exception("Boom");
                using var trx = await kvl.BeginTransactionAsync(key);
                await kvl.Insert(key, "$.world", "[]");
                await kvl.Insert(key, "$.world[#]", i);
                await trx.CommitAsync();
            }
        }
    }
}
