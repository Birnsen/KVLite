
using System;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Threading;
using System.Threading.Tasks;

namespace KVL
{
    public class KVTransaction : IDisposable, IAsyncDisposable
    {
        private DbTransaction _transaction;
        private bool disposedValue;

        private KVTransaction(DbTransaction transaction) 
        {
            _transaction = transaction;
        }

        public static async Task<KVTransaction> BeginTransactionAsync(SQLiteConnection connection)
        {
            var transaction = await connection.BeginTransactionAsync();
            return new KVTransaction(transaction);
        }

        public static KVTransaction BeginTransaction(SQLiteConnection connection)
        {
            return new KVTransaction(connection.BeginTransaction());
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public Task CommitAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _transaction.CommitAsync(cancellationToken);
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public Task RollbackAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _transaction.RollbackAsync(cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            return _transaction.DisposeAsync();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _transaction.Dispose();
                }

                disposedValue= true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}