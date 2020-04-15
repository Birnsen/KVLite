using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;

namespace KVL
{
    internal class KV : KVBase<byte[]>
    {
        public static KV CreateWithFileInfo(FileInfo file)
        {
            return new KV(file.FullName);
        }

        public static KV CreateInMemory()
        {
            return new KV(":memory:");
        }

        private KV(string path) : base(path)
        {
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
    }
}
