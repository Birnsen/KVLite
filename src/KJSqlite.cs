using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace KVL
{
    internal class KJ : KVBase<string>, JsonApi
    {
        public static KJ CreateWithFileInfo(FileInfo file)
        {
            return new KJ(file.FullName);
        }

        public static KJ CreateInMemory()
        {
            return new KJ(":memory:");
        }

        private KJ(string path) : base(path)
        {
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
    }
}
