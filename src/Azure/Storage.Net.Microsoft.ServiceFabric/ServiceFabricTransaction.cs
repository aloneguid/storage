using System;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using SFTransaction = Microsoft.ServiceFabric.Data.ITransaction;

namespace Storage.Net.Microsoft.ServiceFabric
{
   class ServiceFabricTransaction : ITransaction
   {
      private readonly SFTransaction _transaction;
      private bool _commited;
      private readonly Action<bool> _transactionClosed;
      private readonly bool _ignoreCommits;

      public ServiceFabricTransaction(IReliableStateManager stateManager, Action<bool> transactionClosed)
      {
         _transaction = stateManager.CreateTransaction();
         _transactionClosed = transactionClosed;
         _ignoreCommits = false;
      }

      public ServiceFabricTransaction(ServiceFabricTransaction transaction)
      {
         _transaction = transaction.Tx;
         _transactionClosed = null;
         _ignoreCommits = true;
      }

      public SFTransaction Tx => _transaction;

      public Task CommitAsync()
      {
         _commited = true;

         if (_ignoreCommits) return Task.FromResult(true);

         return _transaction.CommitAsync();
      }

      public void Dispose()
      {
         try
         {
            if (!_ignoreCommits)
            {
               if (!_commited) _transaction.Abort();

               _transaction.Dispose();
            }
         }
         finally
         {
            _transactionClosed?.Invoke(true);
         }
      }
   }
}
