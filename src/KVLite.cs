using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System;
using LanguageExt;
using System.Threading;
using MoreLinq;

namespace KVL
{
    public static partial class KVLite
    {
        public static KJApi<string> CreateJsonInMemory()
        {
            return new KVLite<string>();
        }

        public static KJApi<string> CreateJsonInDirectory(DirectoryInfo directory)
        {
            return new KVLite<string>(directory.FullName);
        }

        public static KVApi<byte[]> CreateInDirectory(DirectoryInfo directory)
        {
            return new KVLite<byte[]>(directory.FullName);
        }
        
        public static KVApi<byte[]> CreateInMemory()
        {
            return new KVLite<byte[]>();
        }
    }

    public partial class KVLite<T> : KJApi<T>
    {
        private const uint HASHTABLE_SIZE = 128;
        private readonly KVApi<T>[] _connections;
        private readonly SemaphoreSlim _bulkSemaphore = new SemaphoreSlim(1, 1);

        internal KVLite()
        {
            if(typeof(T) == typeof(byte[]) )
            {
                _connections = createConnections(_ => (KVApi<T>) KeyValue.CreateInMemory());
            }
            else if(typeof(T) == typeof(string) )
            {
                _connections = createConnections(_ => (KVApi<T>) KeyJson.CreateInMemory());
            }
            else
            {
                throw new NotSupportedException($"Type {typeof(T).Name} is not supported!");
            }
        }

        internal KVLite(string directory)
        {
            var filePath = Path.Combine(directory, "kv-{0}");

            if(typeof(T) == typeof(byte[]) )
            {
                _connections = createConnections(x => (KVApi<T>) KeyValue.CreateWithFileInfo(new FileInfo(string.Format(filePath, x))));
            }
            else if(typeof(T) == typeof(string) )
            {
                _connections = createConnections(x => (KVApi<T>) KeyJson.CreateWithFileInfo(new FileInfo(string.Format(filePath, x))));
            }
            else
            {
                throw new NotSupportedException($"Type {typeof(T).Name} is not supported!");
            }
        }

        private KVApi<T>[] createConnections(Func<int, KVApi<T>> factory)
        {
            return Enumerable.Range(0, (int) HASHTABLE_SIZE)
                .Select(x => factory(x))
                .ToArray();
        }

        public KVTransaction BeginTransaction(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            return ((JsonApi) _connections[id]).BeginTransaction(key);
        }

        public async Task<KVTransaction> BeginTransactionAsync(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            return await ((JsonApi) _connections[id]).BeginTransactionAsync(key);
        }

        public async Task Add(byte[] key, T value)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Add(key, value);
        }

        public async Task Add(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            try
            {
                await _bulkSemaphore.WaitAsync();
                var tasks = entries
                    .GroupBy(kvp => FNVHash.Hash(kvp.Key, HASHTABLE_SIZE))
                    .Select(group => Task.Run(async delegate
                    {
                        await _connections[group.Key].Add(group);
                    }));

                await Task.WhenAll(tasks);
            }
            finally
            {
                _bulkSemaphore.Release();
            }
        }

        public async Task Upsert(byte[] key, T value)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Upsert(key, value);
        }

        public async Task Upsert(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            try
            {
                await _bulkSemaphore.WaitAsync();
                var tasks = entries
                    .GroupBy(kvp => FNVHash.Hash(kvp.Key, HASHTABLE_SIZE))
                    .Select(group => Task.Run(async delegate
                    {
                        await _connections[group.Key].Upsert(group);
                    }));

                await Task.WhenAll(tasks);
            }
            finally
            {
                _bulkSemaphore.Release();
            }
        }

        public async Task Update(byte[] key, T value)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Update(key, value);
        }
        
        public async Task Update(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            try
            {
                await _bulkSemaphore.WaitAsync();
                var tasks = entries
                    .GroupBy(kvp => FNVHash.Hash(kvp.Key, HASHTABLE_SIZE))
                    .Select(group => Task.Run(async delegate
                    {
                        await _connections[group.Key].Update(group);
                    }));

                await Task.WhenAll(tasks);
            }
            finally
            {
                _bulkSemaphore.Release();
            }
        }

        public async Task Delete(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Delete(key);
        }

        public async Task Delete(IEnumerable<byte[]> keys)
        {
            try
            {
                await _bulkSemaphore.WaitAsync();
                var tasks = keys
                    .GroupBy(key => FNVHash.Hash(key, HASHTABLE_SIZE))
                    .Select(group => Task.Run(async delegate
                    {
                        await _connections[group.Key].Delete(group);
                    }));

                await Task.WhenAll(tasks);
            }
            finally
            {
                _bulkSemaphore.Release();
            }
        }

        public async Task<long> Count()
        {
            var tasks = _connections
                .Select(c => c.Count());

            return (await Task.WhenAll(tasks)).Sum();
        }

        public async Task<long> Count<S>(string path, Compare comparison, S value)
        {
            var tasks = _connections
                .Select(c =>((JsonApi) c).Count(path, comparison, value));

            return (await Task.WhenAll(tasks)).Sum();
        }

        public async Task<Option<T>> Get(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            return await _connections[id].Get(key);
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> GetRR(bool truncateWal = false)
        {
            var batchSize = (int)HASHTABLE_SIZE / 8;
            var batch = _connections.Batch(batchSize);
            var id = 0;
            foreach(var cons in batch)
            {
                var con = cons
                    .Select((c, i) => (i, c.GetRR(truncateWal).GetAsyncEnumerator())).ToDictionary(kv => kv.i, kv => kv.Item2);

                do
                {
                    var i = Math.Abs(id % batchSize);
                    if(con.ContainsKey(i) && await con[i].MoveNextAsync())
                    {
                        yield return con[i].Current;
                    }
                    else 
                    {
                        con.Remove(i);
                    }

                    ++id;
                }
                while (con.Any());
            }
        }


        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get(bool truncateWal = false)
        {
            var counter = 0;
            foreach(var c in _connections)
            {
                await foreach(var kv in c.Get(truncateWal))
                {
                    yield return kv;
                }
                ++counter;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get<T, S>(string path, Compare comparison, S value, bool truncateWal = false)
        {
            foreach(var c in _connections)
            {
                await foreach(var kv in ((JsonApi) c).Get<T, S>(path, comparison, value, truncateWal))
                {
                    yield return kv;
                }
            }
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> GetRR<T, S>(string path, Compare comparison, S value, bool truncateWal = false)
        {

            var batchSize = (int)HASHTABLE_SIZE / 8;
            var batch = _connections.Batch(batchSize);
            var id = 0;
            foreach (var cons in batch)
            {
                var con = cons
                    .Select((c, i) => (i, ((JsonApi)c).GetRR<T, S>(path, comparison, value, truncateWal).GetAsyncEnumerator())).ToDictionary(kv => kv.i, kv => kv.Item2);
                do
                {
                    var i = Math.Abs(id % batchSize);
                    if (con.ContainsKey(i) && await con[i].MoveNextAsync())
                    {
                        yield return con[i].Current;
                    }
                    else
                    {
                        con.Remove(i);
                    }

                    ++id;
                }
                while (con.Any());
            }
        }

        public async Task Clean()
        {
            foreach(var c in _connections)
            {
                await c.Clean();
            }
        }

        public async Task Insert<S>(byte[] key, string path, S jsonToInsert)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await ((JsonApi) _connections[id]).Insert(key, path, jsonToInsert);
        }

        public async Task Replace<S>(byte[] key, string path, S jsonToReplace)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await ((JsonApi) _connections[id]).Replace(key, path, jsonToReplace);
        }

        public async Task Set<S>(byte[] key, string path, S jsonToSet)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await ((JsonApi) _connections[id]).Set(key, path, jsonToSet);
        }

        public async Task Remove(byte[] key, string path)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await ((JsonApi) _connections[id]).Remove(key, path);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    foreach(var c in _connections)
                    {
                        c.Dispose();
                    }
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

}
