using System;
using System.Linq;
using System.IO;
using System.Text;
using KVL;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;

namespace benchmark
{
    public static class H
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int batch)
        {
            var nextBatch = new List<T>(batch);
            foreach(var item in source)
            {
                nextBatch.Add(item);
                if(nextBatch.Count == batch)
                {
                    yield return nextBatch;
                    nextBatch = new List<T>();
                }
            }

            if(nextBatch.Count > 0)
                yield return nextBatch;
        }
    }

    class Program
    {
        private static Random RND = new Random(Guid.NewGuid().GetHashCode());

        private static int Count = 100000;

        private static async Task GetBench(KVApi<byte[]> kvl)
        {
            Console.WriteLine("Starting get bench");
            var sw = Stopwatch.StartNew();
            var counter = 0;
            await foreach(var k in kvl.Get())
            {
                counter++;
                if(counter % 10000 == 0) Console.WriteLine($"{counter}");
            }
            sw.Stop();
            Console.WriteLine($"Runtime: {sw.Elapsed}");
        }
        
        private static async Task BatchedBench(KVApi<byte[]> kvl, byte[] value)
        {
            var data = Enumerable.Range(0, Count)
                .Select(i => new KeyValuePair<byte[], byte[]>(Encoding.UTF8.GetBytes($"key{i}"), value))
                .Batch(10000)
                .ToList();

            Console.WriteLine("Starting batched bench");
            var sw = Stopwatch.StartNew();
            var counter = 0;
            foreach(var entries in data)
            {
                counter++;
                await kvl.Add(entries);
                if(counter % 10000 == 0) Console.WriteLine($"{counter}");
            }
            sw.Stop();
            Console.WriteLine($"Runtime: {sw.Elapsed}");
        }

        private static async Task ParallelBench(KVApi<byte[]> kvl, byte[] value, Func<byte[],byte[], Task> func)
        {
            var data = Enumerable.Range(0, Count)
                .Select(i => Encoding.UTF8.GetBytes($"key{i}"))
                .ToList();

            var counter = 0;
            var parts = Partitioner
                .Create(data)
                .GetPartitions(8)
                .Select(keyList => Task.Run(async delegate {
                    using var iter = keyList;
                    while(iter.MoveNext())
                    {
                        Interlocked.Increment(ref counter);
                        if(counter % 1000 == 0) Console.WriteLine($"{counter}");
                        await func(iter.Current, value);
                    }
                }));

            Console.WriteLine("Starting parallel bench");
            var sw = Stopwatch.StartNew();

            await Task.WhenAll(parts);
            
            sw.Stop();
            Console.WriteLine($"Runtime: {sw.Elapsed}");
        }

        static async Task Main(string[] args)
        {
            var value = new byte[1024];
            RND.NextBytes(value);
            
            var benchFolder = new DirectoryInfo("./bench");
            benchFolder.Delete(true);
            var kvliteFolder = benchFolder.CreateSubdirectory("kvlite");

            using(var kvl = KVLite.CreateInDirectory(kvliteFolder))
            {
                await BatchedBench(kvl, value);
            }

            kvliteFolder.Delete(true);
            kvliteFolder.Create();

            using(var kvl = KVLite.CreateInDirectory(kvliteFolder))
            {
                Console.WriteLine("Add:");
                await ParallelBench(kvl, value, kvl.Add);
                Console.WriteLine("Update:");
                RND.NextBytes(value);
                await ParallelBench(kvl, value, kvl.Update);
                Console.WriteLine("Get:");
                await GetBench(kvl);
                Console.WriteLine("Delete:");
                await ParallelBench(kvl, value, (k, _) => kvl.Delete(k));
            }

            /*
            kvliteFolder.Delete(true);
            kvliteFolder.Create();

            using(var kvl = KVLite.CreateInDirectory(kvliteFolder))
            {
                await SequenceBench(kvl);
            }//*/



        }

        private static async Task SequenceBench(KVApi<byte[]> kvl)
        {
            var value = new byte[1024];
            var data = Enumerable.Range(0, 100000)
                .Select(i => Encoding.UTF8.GetBytes($"key{i}"))
                .ToList();

            Console.WriteLine("Starting sequence bench");
            var sw = Stopwatch.StartNew();
            var counter = 0;
            foreach(var entry in data)
            {
                counter++;
                await kvl.Add(entry, value);
                if(counter % 1000 == 0) Console.WriteLine($"{counter}");
            }
            sw.Stop();
            Console.WriteLine($"Runtime: {sw.Elapsed}");
        }
    }
}
