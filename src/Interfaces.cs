using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LanguageExt;

namespace KVL
{
    public interface KVApi<T> : IDisposable
    {
        Task Add(byte[] key, T value);
        Task Add(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task AddorRep(byte[] key, T value);
        Task AddorRep(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task Update(byte[] key, T value);
        Task Update(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task Delete(byte[] key);
        Task Delete(IEnumerable<byte[]> keys);
        Task<Option<T>> Get(byte[] key);
        IAsyncEnumerable<KeyValuePair<byte[], T>> Get();
    }

    public interface JsonApi
    {
        KVTransaction BeginTransaction(byte[] key);
        Task<KVTransaction> BeginTransactionAsync(byte[] key);
        Task Insert<T>(byte[] key, string path, T jsonToInsert);
        Task Replace<T>(byte[] key, string path, T jsonToReplace);
        Task Set<T>(byte[] key, string path, T jsonToSet);
        Task Remove(byte[] key, params string[] path);
    }

    public interface KJApi<T> : KVApi<T>, JsonApi
    {}
}
