
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.Json;
using System.Linq;

namespace KVL.Tests
{
    [TestClass]
    public class LifecycleTests
    {
        [TestMethod]
        public async Task TestAdd()
        {
            var kvl = KVLite.CreateJsonInMemory();

            Parallel.ForEach(Enumerable.Range(0, 100), async x => {

                for(var i = 0; i < 1000; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                    var value = JsonSerializer.Serialize(new {hello = "value"});
                    await kvl.Add(key, value);
                }

                for(var i = 0; i < 1000; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                    await kvl.Insert(key, "$.world", "value");
                }

                for(var i = 0; i < 1000; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                    await kvl.Insert(key, "$.world", "value");
                    var res = await kvl.Get(key);

                    var expected = JsonSerializer.Serialize(new {hello = "value", world = "value"});
                    Assert.AreEqual(expected, res);
                }

                for(var i = 0; i < 1000; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                    await kvl.Delete(key);
                    await Assert.ThrowsExceptionAsync<Exception>(() => kvl.Get(key), "Key not found!");
                }
            });

            var count = await kvl.Get().CountAsync();

            Assert.AreEqual(0, count);

        }
    }
}
