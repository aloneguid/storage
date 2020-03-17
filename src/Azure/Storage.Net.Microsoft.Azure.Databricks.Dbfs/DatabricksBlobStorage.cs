using System;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class DatabricksBlobStorage : VirtualStorage
   {
      public DatabricksBlobStorage(string baseUri, string token)
      {
         if(baseUri is null)
            throw new ArgumentNullException(nameof(baseUri));
         if(token is null)
            throw new ArgumentNullException(nameof(token));

         var client = DatabricksClient.CreateClient(baseUri, token);

         Mount("dbfs", new DbfsStorage(client.Dbfs));
      }
   }
}
