using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Threading;

namespace KVL
{
    internal class KeyJson : KVBase<string>, JsonApi
    {
        private readonly SemaphoreSlim _transactionSemaphore = new SemaphoreSlim(1, 1);

        public static KeyJson CreateWithFileInfo(FileInfo file)
        {
            return new KeyJson(file.FullName);
        }

        public static KeyJson CreateInMemory()
        {
            return new KeyJson(":memory:");
        }

        private KeyJson(string path) : base(path)
        {
            _connection.EnableExtensions(true);

            var extPath = "./runtimes/{0}/native/netstandard2.0/SQLite.Interop.dll";
            var system = "linux-x64";

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                system = "osx-x64";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                system = RuntimeInformation.OSArchitecture == Architecture.X64
                    ? "win-x64"
                    : "win-x86";
            }

            _connection.LoadExtension(string.Format(extPath, system), "sqlite3_json_init");

            createTable();
        }

        private void createTable()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                CREATE TABLE IF NOT EXISTS {nameof(keyvaluestore)}(
                    {keyvaluestore.rowid} INTEGER PRIMARY KEY AUTOINCREMENT,
                    {keyvaluestore.key} BLOB UNIQUE,
                    {keyvaluestore.value} TEXT
                )";
            _ = cmd.ExecuteNonQuery();
        }

        public KVTransaction BeginTransaction(byte[] _)
        {
            return KVTransaction.BeginTransaction(_connection, _transactionSemaphore);
        }

        public async Task<KVTransaction> BeginTransactionAsync(byte[] _)
        {
            await _transactionSemaphore.WaitAsync();
            return await KVTransaction.BeginTransactionAsync(_connection, _transactionSemaphore);
        }

        public override async Task Add(byte[] key, string value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT OR IGNORE INTO {nameof(keyvaluestore)} (
                    {keyvaluestore.key},
                    {keyvaluestore.value}
                    ) VALUES (@key, json(@value))
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public override async Task Upsert(byte[] key, string value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT OR REPLACE INTO {nameof(keyvaluestore)} (
                    {keyvaluestore.key},
                    {keyvaluestore.value}
                    ) VALUES (@key, json(@value))
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public override async Task Update(byte[] key, string value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = json(@value) 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Insert<T>(byte[] key, string path, T jsonToInsert)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_insert({keyvaluestore.value}, @path, json(@value))
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", jsonToInsert);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Replace<T>(byte[] key, string path, T jsonToReplace)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_replace({keyvaluestore.value}, @path, json(@value))
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", jsonToReplace);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Set<T>(byte[] key, string path, T jsonToSet)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_set({keyvaluestore.value}, @path, json(@value))
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", jsonToSet);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Remove(byte[] key, string path)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_remove({keyvaluestore.value}, @path)
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", path);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        private static string FromComparison(Compare comparison) =>
        comparison switch {
            Compare.EQ => "==",
            Compare.NE => "<>",
            Compare.GE => ">=",
            Compare.LE => "<=",
            Compare.GT => ">",
            Compare.LT => "<",
            _ => throw new ArgumentException("Invalid enum value", nameof(comparison))
        };

        public async Task<long> Count<T>(string path, Compare comparison, T value)
        {
            var comp = FromComparison(comparison);
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT count() FROM {nameof(keyvaluestore)}
                WHERE json_extract({keyvaluestore.value}, @path) {comp} @value 
                ";

            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", value);

            return (long)await cmd.ExecuteScalarAsync();
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get<T, S>(string path, Compare comparison, S value)
        {
            var pageCounter = 0;
            var entryCounter = 0;
            do
            {
                entryCounter = 0;
                await foreach (var kv in get<T, S>(pageCounter * 512, 512, path, comparison, value))
                {
                    entryCounter++;
                    yield return kv;
                }

                pageCounter++;
            } while (entryCounter > 0);
        }

        private async IAsyncEnumerable<KeyValuePair<byte[], T>> get<T, S>(long page, int maxSize, string path, Compare comparison, S value)
        {
            //Propably faster then LIMIT/OFFSET as per: http://blog.ssokolow.com/archives/2009/12/23/sql-pagination-without-offset/
            var comp = FromComparison(comparison);
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT * FROM {nameof(keyvaluestore)} 
                WHERE json_extract({keyvaluestore.value}, @path) {comp} @value 
                AND rowid NOT IN (
                    SELECT rowid FROM {nameof(keyvaluestore)}
                    ORDER BY rowid ASC LIMIT {page} 
                )
                ORDER BY rowid ASC LIMIT {maxSize}
                ";

            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", value);

            var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var key = (byte[])reader.GetValue(1);
                var retValue = (T)reader.GetValue(2);

                yield return new KeyValuePair<byte[], T>(key, retValue);
            }
        }
    }
}
