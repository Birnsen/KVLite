
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

            var run = Enumerable.Range(0, 10)
                .Select(x => lifecycle(kvl, x));

            await Task.WhenAll(run);

            var count = await kvl.Get().CountAsync();

            Assert.AreEqual(0, count);

        }

        private async Task lifecycle(KJApi<string> kvl, int x)
        {
            var jsonStringValue = JsonSerializer.Serialize("value");
        
            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                var value = JsonSerializer.Serialize(new {hello = "value"});
                await kvl.Add(key, value);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                await kvl.Insert(key, "$.world", jsonStringValue);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                await kvl.Insert(key, "$.world", jsonStringValue);
                var res = await kvl.Get(key);

                var expected = JsonSerializer.Serialize(new {hello = "value", world = "value"});
                Assert.AreEqual(expected, res);
            }

            for(var i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{x}-{i}");
                await kvl.Delete(key);
                var res = await kvl.Get(key);
                Assert.IsTrue(res.IsNone);
            }
        }
    }
}
