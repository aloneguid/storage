using System;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class DatabricksBlobStorage : VirtualStorage, IDatabricksStorage
   {
      private readonly DatabricksClient _nativeClient;
      private readonly SecretStorage _ss;

      public DatabricksBlobStorage(string baseUri, string token)
      {
         if(baseUri is null)
            throw new ArgumentNullException(nameof(baseUri));
         if(token is null)
            throw new ArgumentNullException(nameof(token));

         _nativeClient = DatabricksClient.CreateClient(baseUri, token);

         Mount("dbfs", new DbfsStorage(_nativeClient.Dbfs));
         Mount("workspace", new WorkspaceStorage(_nativeClient.Workspace));
         Mount("secrets", _ss = new SecretStorage(_nativeClient.Secrets));
      }

      public DatabricksClient GetMicrosoftAzureDatabrickClientLibraryClient() => _nativeClient;

      public Task CreateSecretsScope(string name, string initialManagePrincipal = null)
      {
         return _ss.CreateSecretsScope(name, initialManagePrincipal);

      }
   }
}
