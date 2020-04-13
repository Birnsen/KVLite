using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace KVL
{
    internal class KJSqlite : KJApi<string>
    {
        private readonly SQLiteConnection _connection;

        public static KJSqlite CreateWithFileInfo(FileInfo file)
        {
            return new KJSqlite(file.FullName);
        }

        public static KJSqlite CreateInMemory()
        {
            return new KJSqlite(":memory:");
        }

        private KJSqlite(string path)
        {
            var builder = new SQLiteConnectionStringBuilder
            {
                DataSource = path,
                JournalMode = SQLiteJournalModeEnum.Wal,
                Version = 3
            };
            
            _connection = new SQLiteConnection(builder.ToString());
            _connection.Open();

            _connection.EnableExtensions(true);

            var extPath = "./runtimes/{0}/native/netstandard2.0/SQLite.Interop.dll";
            var system = "linux-x64";

            if(RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                system = "osx-x64";
            }
            else if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
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

        public async Task Add(byte[] key, string value)
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

        public async Task Add(IEnumerable<KeyValuePair<byte[], string>> entries)
        {
            using var trx = _connection.BeginTransaction();
            foreach(var e in entries)
            {
                await Add(e.Key, e.Value);
            }
            trx.Commit();
        }

        public async Task Update(byte[] key, string value)
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

        public async Task Update(IEnumerable<KeyValuePair<byte[],string>> entries)
        {
            using var trx = _connection.BeginTransaction();
            foreach(var e in entries)
            {
                await Update(e.Key, e.Value);
            }
            trx.Commit();
        }

        public async Task Delete(byte[] key)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                DELETE FROM {nameof(keyvaluestore)} 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Delete(IEnumerable<byte[]> keys)
        {
            using var trx = _connection.BeginTransaction();
            foreach(var k in keys)
            {
                await Delete(k);
            }
            trx.Commit();
        }

        public async Task<string> Get(byte[] key)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT {keyvaluestore.value} FROM {nameof(keyvaluestore)} 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            var ret = await cmd.ExecuteScalarAsync();

            return (string) ret ?? throw new Exception("Key not found!");
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], string>> Get()
        {
            var pageCounter = 0;
            var entryCounter = 0;
            do
            {
                entryCounter = 0;
                await foreach(var kv in get(pageCounter * 512, 512))
                {
                    entryCounter++;
                    yield return kv;
                }   

                pageCounter++;
            } while(entryCounter > 0);
        }

        private async IAsyncEnumerable<KeyValuePair<byte[], string>> get(long page, int maxSize)
        {
            //Propably faster then LIMIT/OFFSET as per: http://blog.ssokolow.com/archives/2009/12/23/sql-pagination-without-offset/
            //TODO Benchmark
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT * FROM {nameof(keyvaluestore)} 
                WHERE rowid NOT IN (
                    SELECT rowid FROM {nameof(keyvaluestore)}
                    ORDER BY rowid ASC LIMIT {page} 
                )
                ORDER BY rowid ASC LIMIT {maxSize}
                ";

            var reader = await cmd.ExecuteReaderAsync();

            while(await reader.ReadAsync())
            {
                var key = (byte[])reader.GetValue(1);
                var value = reader.GetString(2);

                yield return new KeyValuePair<byte[], string>(key, value);
            }
        }

        public async Task Insert<T>(byte[] key, string path, T jsonToInsert)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_insert({keyvaluestore.value}, @path, @value)
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
                    SELECT json_replace({keyvaluestore.value}, @path, @value)
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
                    SELECT json_set({keyvaluestore.value}, @path, @value)
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", path);
            cmd.Parameters.AddWithValue("value", jsonToSet);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Remove(byte[] key, params string[] path)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = (
                    SELECT json_remove({keyvaluestore.value}, @path, @value)
                    FROM {nameof(keyvaluestore)}
                    WHERE key = @key)
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("path", string.Join(",", path));

            _ = await cmd.ExecuteNonQueryAsync();
        }

        
        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _connection.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        enum keyvaluestore
        {
            rowid,
            key,
            value

        }
    }


}
