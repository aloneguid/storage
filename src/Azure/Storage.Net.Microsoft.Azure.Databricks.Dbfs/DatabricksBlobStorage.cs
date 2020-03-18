using System;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class DatabricksBlobStorage : VirtualStorage, IDatabricksStorage
   {
      private readonly DatabricksClient _nativeClient;

      public DatabricksBlobStorage(string baseUri, string token)
      {
         if(baseUri is null)
            throw new ArgumentNullException(nameof(baseUri));
         if(token is null)
            throw new ArgumentNullException(nameof(token));

         _nativeClient = DatabricksClient.CreateClient(baseUri, token);

         Mount("dbfs", new DbfsStorage(_nativeClient.Dbfs));
         Mount("workspace", new WorkspaceStorage(_nativeClient.Workspace));
      }

      public DatabricksClient GetMicrosoftAzureDatabrickClientLibraryClient() => _nativeClient;
   }
}
