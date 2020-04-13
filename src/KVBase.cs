
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;

namespace KVL
{
    internal class KVBase<T> : KVApi<T>
    {
        private readonly SQLiteConnection _connection;

        public static KVBase<T> CreateWithFileInfo(FileInfo file)
        {
            return new KVBase<T>(file.FullName);
        }

        public static KVBase<T> CreateInMemory()
        {
            return new KVBase<T>(":memory:");
        }

        private KVBase(string path)
        {
            var builder = new SQLiteConnectionStringBuilder
            {
                DataSource = path,
                JournalMode = SQLiteJournalModeEnum.Wal,
                Version = 3
            };
            
            _connection = new SQLiteConnection(builder.ToString());
            _connection.Open();

            createTable();
        }

        private void createTable()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                CREATE TABLE IF NOT EXISTS {nameof(keyvaluestore)}(
                    {keyvaluestore.rowid} INTEGER PRIMARY KEY AUTOINCREMENT,
                    {keyvaluestore.key} BLOB UNIQUE,
                    {keyvaluestore.value} BLOB
                )";
            _ = cmd.ExecuteNonQuery();
        }

        public async Task Add(byte[] key, T value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT OR IGNORE INTO {nameof(keyvaluestore)} (
                    {keyvaluestore.key},
                    {keyvaluestore.value}
                    ) VALUES (@key, @value)
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Add(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            using var trx = _connection.BeginTransaction();
            foreach(var e in entries)
            {
                await Add(e.Key, e.Value);
            }
            trx.Commit();
        }

        public async Task Update(byte[] key, T value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {nameof(keyvaluestore)} 
                SET {keyvaluestore.value} = @value 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Update(IEnumerable<KeyValuePair<byte[], T>> entries)
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

        public async Task<T> Get(byte[] key)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT {keyvaluestore.value} FROM {nameof(keyvaluestore)} 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            var ret = await cmd.ExecuteScalarAsync();

            return (T) ret ?? throw new Exception("Key not found!");
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get()
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

        private async IAsyncEnumerable<KeyValuePair<byte[], T>> get(long page, int maxSize)
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
                var value = (T)reader.GetValue(2);

                yield return new KeyValuePair<byte[], T>(key, value);
            }
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
