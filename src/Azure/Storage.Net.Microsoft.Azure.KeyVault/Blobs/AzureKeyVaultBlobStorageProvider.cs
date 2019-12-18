using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using System.Text.RegularExpressions;
using NetBox.Extensions;
using NetBox;
using System.Net;
using Storage.Net.Blobs;
using Azure.Security.KeyVault.Secrets;
using Azure.Core;
using Azure;

namespace Storage.Net.Microsoft.Azure.KeyVault.Blobs
{
   class AzureKeyVaultBlobStorageProvider : IBlobStorage
   {
      private readonly SecretClient _client;
      private readonly string _vaultUri;
      private static readonly Regex secretNameRegex = new Regex("^[0-9a-zA-Z-]+$");

      public AzureKeyVaultBlobStorageProvider(Uri vaultUri, TokenCredential tokenCredential)
      {
         _client = new SecretClient(vaultUri, tokenCredential);

         _vaultUri = vaultUri.ToString().Trim('/');
      }

      #region [ IBlobStorage ]

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         if (!StoragePath.IsRootPath(options.FolderPath)) return new List<Blob>();

         var secrets = new List<Blob>();

         await foreach(SecretProperties secretProperties in _client.GetPropertiesOfSecretsAsync(cancellationToken).ConfigureAwait(false))
         {
            Blob blob = ToBlob(secretProperties);
            if(!options.IsMatch(blob))
               continue;

            if(options.BrowseFilter != null && !options.BrowseFilter(blob))
               continue;

            secrets.Add(blob);

            if(options.MaxResults != null && secrets.Count >= options.MaxResults.Value)
               break;
         }

         return secrets;
      }

      private static Blob ToBlob(SecretProperties secretProperties)
      {
         var blob = new Blob(secretProperties.Name, BlobItemKind.File);
         blob.LastModificationTime = secretProperties.UpdatedOn;

         blob.TryAddProperties(
            "ContentType", secretProperties.ContentType,
            "CreatedOn", secretProperties.CreatedOn,
            "IsEnabled", secretProperties.Enabled,
            "ExpiresOn", secretProperties.ExpiresOn,
            "Id", secretProperties.Id,
            "KeyId", secretProperties.KeyId,
            "IsManaged", secretProperties.Managed,
            "NotBefore", secretProperties.NotBefore,
            "RecoveryLevel", secretProperties.RecoveryLevel,
            "Tags", secretProperties.Tags,
            "UpdatedOn", secretProperties.UpdatedOn,
            "VaultUri", secretProperties.VaultUri,
            "Version", secretProperties.Version,
            "IsSecret", true);

         return blob;
      }

      public async Task WriteAsync(string fullPath, Stream dataStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         ValidateSecretName(fullPath);
         if (append) throw new ArgumentException("appending to secrets is not supported", nameof(append));

         byte[] data = dataStream.ToByteArray();
         string value = Encoding.UTF8.GetString(data);
         await _client.SetSecretAsync(fullPath, value, cancellationToken).ConfigureAwait(false);
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         ValidateSecretName(fullPath);

         try
         {
            Response<KeyVaultSecret> secret = await _client.GetSecretAsync(fullPath, cancellationToken: cancellationToken).ConfigureAwait(false);

            string value = secret.Value.Value;

            return value.ToMemoryStream();
         }
         catch(RequestFailedException ex) when (ex.Status == 404)
         {
            return null;
         }
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         await Task.WhenAll(fullPaths.Select(fullPath => DeleteAsync(fullPath, cancellationToken))).ConfigureAwait(false);
      }

      private async Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         try
         {
            await _client.StartDeleteSecretAsync(fullPath, cancellationToken).ConfigureAwait(false);
         }
         catch(RequestFailedException ex) when(ex.Status == 404)
         {

         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         return await Task.WhenAll(fullPaths.Select(fullPath => ExistsAsync(fullPath))).ConfigureAwait(false);
      }

      private async Task<bool> ExistsAsync(string fullPath)
      {
         try
         {
            await _client.GetSecretAsync(fullPath).ConfigureAwait(false);
         }
         catch(RequestFailedException ex) when(ex.Status == 404)
         {
            return false;
         }

         return true;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         return await Task.WhenAll(fullPaths.Select(fullPath => GetBlobAsync(fullPath))).ConfigureAwait(false);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      private async Task<Blob> GetBlobAsync(string fullPath)
      {
         try
         {
            Response<KeyVaultSecret> secret = await _client.GetSecretAsync(fullPath).ConfigureAwait(false);

            return ToBlob(secret.Value.Properties);
         }
         catch(RequestFailedException ex) when(ex.Status == 404)
         {
            return null;
         }
      }

      #endregion

      private static void ValidateSecretName(string fullPath)
      {
         if(!secretNameRegex.IsMatch(fullPath))
         {
            throw new NotSupportedException($"secret '{fullPath}' does not match expected pattern '^[0-9a-zA-Z-]+$'");
         }
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public Task MoveBlobAsync(string fromPath, string toPath, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }
   }
}
