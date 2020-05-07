
using System;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Threading;
using System.Threading.Tasks;

namespace KVL
{
    public class KVTransaction : IDisposable
    {
        private DbTransaction _transaction;
        private bool disposedValue;
        private readonly SemaphoreSlim _transactionSemaphore;

        private KVTransaction(DbTransaction transaction, SemaphoreSlim trxSemaphore) 
        {
            _transaction = transaction;
            _transactionSemaphore = trxSemaphore;
        }

        public static async Task<KVTransaction> BeginTransactionAsync(SQLiteConnection connection, SemaphoreSlim trxSemaphore)
        {
            try
            {
                var transaction = await connection.BeginTransactionAsync();
                return new KVTransaction(transaction, trxSemaphore);
            }
            catch(Exception ex)
            {
                Console.WriteLine($"{ex}");
                trxSemaphore.Release();
                throw;
            }
        }

        public static KVTransaction BeginTransaction(SQLiteConnection connection, SemaphoreSlim trxSemaphore)
        {
            try
            {
                trxSemaphore.Wait();
                return new KVTransaction(connection.BeginTransaction(), trxSemaphore);
            }
            catch(Exception)
            {
                trxSemaphore.Release();
                throw;
            }
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public async Task CommitAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await _transaction.CommitAsync(cancellationToken);
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public async Task RollbackAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await _transaction.RollbackAsync(cancellationToken);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _transaction.Dispose();
                    _transactionSemaphore.Release();
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