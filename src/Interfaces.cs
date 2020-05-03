﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LanguageExt;

namespace KVL
{
    public interface KVApi<T> : IDisposable
    {
        Task Add(byte[] key, T value);
        Task Add(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task Upsert(byte[] key, T value);
        Task Upsert(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task Update(byte[] key, T value);
        Task Update(IEnumerable<KeyValuePair<byte[], T>> entries);
        Task Delete(byte[] key);
        Task Delete(IEnumerable<byte[]> keys);
        Task<long> Count();
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
        Task<long> Count<T>(string path, Compare comparison, T value);
    }

    public enum Compare
    {
        EQ,
        NE,
        GT,
        LT,
        GE,
        LE
    }

    public interface KJApi<T> : KVApi<T>, JsonApi
    {}
}
