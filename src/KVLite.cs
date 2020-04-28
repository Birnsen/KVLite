using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System;
using LanguageExt;

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
            var tasks = entries
                .GroupBy(kvp => FNVHash.Hash(kvp.Key, HASHTABLE_SIZE))
                .Select(group => Task.Run(async delegate 
                {
                    await _connections[group.Key].Add(group);
                }));

            await Task.WhenAll(tasks);
        }

        public async Task Update(byte[] key, T value)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Update(key, value);
        }
        
        public async Task Update(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            var tasks = entries
                .GroupBy(kvp => FNVHash.Hash(kvp.Key, HASHTABLE_SIZE))
                .Select(group => Task.Run(async delegate 
                {
                    await _connections[group.Key].Update(group);
                }));

            await Task.WhenAll(tasks);
        }

        public async Task Delete(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            await _connections[id].Delete(key);
        }

        public async Task Delete(IEnumerable<byte[]> keys)
        {
            var tasks = keys
                .GroupBy(key => FNVHash.Hash(key, HASHTABLE_SIZE))
                .Select(group => Task.Run(async delegate 
                {
                    await _connections[group.Key].Delete(group);
                }));

            await Task.WhenAll(tasks);
        }

        public async Task<Option<T>> Get(byte[] key)
        {
            var id = FNVHash.Hash(key, HASHTABLE_SIZE);
            return await _connections[id].Get(key);
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get()
        {
            foreach(var c in _connections)
            {
                await foreach(var kv in c.Get())
                {
                    yield return kv;
                }
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

        public async Task Remove(byte[] key, params string[] path)
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
