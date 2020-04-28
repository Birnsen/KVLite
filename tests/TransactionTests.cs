
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;

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
    }
}
