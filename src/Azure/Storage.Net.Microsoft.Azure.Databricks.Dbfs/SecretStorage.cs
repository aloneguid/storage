using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using NetBox.Extensions;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class SecretStorage : GenericBlobStorage
   {
      private readonly ISecretsApi _api;

      public SecretStorage(ISecretsApi api)
      {
         _api = api;
      }

      public async Task CreateSecretsScope(string name, string initialManagePrincipal = null)
      {
         try
         {
            await _api.CreateScope(name, initialManagePrincipal);
         }
         catch(ClientApiException ex) when(ex.StatusCode == HttpStatusCode.BadRequest)
         {
            //no other reliable way to detect ERROR_RESOURCE_ALREADY_EXISTS
         }
      }


      protected override bool CanListHierarchy => false;

      protected override async Task<IReadOnlyCollection<Blob>> ListAtAsync(
         string path, ListOptions options, CancellationToken cancellationToken)
      {

         if(StoragePath.IsRootPath(path))
         {
            var r = new List<Blob>();
            IEnumerable<SecretScope> scopes = await _api.ListScopes();
            foreach(SecretScope scope in scopes)
            {
               var scopeBlob = new Blob(scope.Name, BlobItemKind.Folder);
               scopeBlob.TryAddProperties(
                  "Backend", scope.BackendType);

               IEnumerable<AclItem> acl = await _api.ListSecretAcl(scope.Name);
               scopeBlob.Properties.Add("ACL", string.Join(";", acl.Select(a => $"{a.Principal}:{a.Permission}")));
               r.Add(scopeBlob);
            }
            return r;
         }

         return null;
      }

      public override async Task WriteAsync(
         string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default)
      {
         GetScopeAndKey(fullPath, out string scope, out string key);
         byte[] data = dataStream.ToByteArray();

         await _api.PutSecret(data, scope, key);
      }

      public override Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         //just a sanity check
         GetScopeAndKey(fullPath, out _, out _);

         // return phrase "[REDACTED]" as there is no technical capability to read secret values in Databricks
         return Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes("[REDACTED]")));
      }

      private static void GetScopeAndKey(string path, out string scope, out string key)
      {
         string[] parts = StoragePath.Split(path);
         if(parts.Length != 2)
            throw new ArgumentException($"path should contain exactly scope and secret name", nameof(path));

         scope = parts[0];
         key = parts[1];
      }
   }
}
