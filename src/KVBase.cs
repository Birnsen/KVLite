
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Threading.Tasks;
using LanguageExt;
using static LanguageExt.Prelude;

namespace KVL
{
    internal class KVBase<T> : KVApi<T>
    {
        protected readonly SQLiteConnection _connection;

        protected KVBase(string path)
        {
            var builder = new SQLiteConnectionStringBuilder
            {
                DataSource = path,
                JournalMode = SQLiteJournalModeEnum.Wal,
                Version = 3
            };

            _connection = new SQLiteConnection(builder.ToString());
            _connection.Open();
        }

        public virtual async Task Add(byte[] key, T value)
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
            foreach (var e in entries)
            {
                await Add(e.Key, e.Value);
            }
            await trx.CommitAsync();
        }


        public virtual async Task Upsert(byte[] key, T value)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT OR REPLACE INTO {nameof(keyvaluestore)} (
                    {keyvaluestore.key},
                    {keyvaluestore.value}
                    ) VALUES (@key, @value)
                ";

            cmd.Parameters.AddWithValue("key", key);
            cmd.Parameters.AddWithValue("value", value);

            _ = await cmd.ExecuteNonQueryAsync();
        }

        public async Task Upsert(IEnumerable<KeyValuePair<byte[], T>> entries)
        {
            using var trx = _connection.BeginTransaction();
            foreach (var e in entries)
            {
                await Upsert(e.Key, e.Value);
            }
            await trx.CommitAsync();
        }

        public virtual async Task Update(byte[] key, T value)
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
            foreach (var e in entries)
            {
                await Update(e.Key, e.Value);
            }
            await trx.CommitAsync();
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
            foreach (var k in keys)
            {
                await Delete(k);
            }
            await trx.CommitAsync();
        }

        public async Task<long> Count()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT count() FROM {nameof(keyvaluestore)} 
                ";

            return (long)await cmd.ExecuteScalarAsync();
        }

        public async Task<Option<T>> Get(byte[] key)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT {keyvaluestore.value} FROM {nameof(keyvaluestore)} 
                WHERE {keyvaluestore.key} = @key
                ";

            cmd.Parameters.AddWithValue("key", key);
            var ret = await cmd.ExecuteScalarAsync();



            return ret != null ? Some((T)ret) : None;
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> Get(bool truncateWal = false)
        {
            await foreach (var kv in get())
            {
                yield return kv;
            }

            if (truncateWal) await Clean();
        }

        public async IAsyncEnumerable<KeyValuePair<byte[], T>> GetRR(bool truncateWal = false)
        {
            await foreach (var kv in get())
            {
                yield return kv;
            }

            if (truncateWal) await Clean();
        }

        public async Task Clean()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"PRAGMA wal_checkpoint(TRUNCATE);";

            var _ = await cmd.ExecuteScalarAsync();
        }

        private async IAsyncEnumerable<KeyValuePair<byte[], T>> get()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"SELECT * FROM {nameof(keyvaluestore)}";

            var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
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

        public enum keyvaluestore
        {
            rowid,
            key,
            value

        }
    }


}
