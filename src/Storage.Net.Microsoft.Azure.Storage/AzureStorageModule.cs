using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.ConnectionString;

namespace Storage.Net.Microsoft.Azure.Storage
{
   class AzureStorageModule : IExternalModule
   {
      public IConnectionFactory ConnectionFactory
      {
         get
         {
            return new ConnectionFactory();
         }
      }
   }
}
