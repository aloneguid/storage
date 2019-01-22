using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.ConnectionString;

namespace Storage.Net.Microsoft.Azure.DataLake.Store
{
   class ExternalModule : IExternalModule
   {
      public IConnectionFactory ConnectionFactory => new ConnectionFactory();
   }
}
