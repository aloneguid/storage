using Microsoft.Azure.KeyVault;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Rest.Azure;
using System.Threading;
using System.Text.RegularExpressions;
using NetBox.Extensions;
using NetBox;
using System.Net;
using Storage.Net.Streaming;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.KeyVault.Blobs
{
   class AzureKeyVaultBlobStorageProvider : IBlobStorage
   {
      private readonly KeyVaultClient _vaultClient;
      private readonly ClientCredential _credential;
      private readonly string _vaultUri;
      private static readonly Regex secretNameRegex = new Regex("^[0-9a-zA-Z-]+$");

      public AzureKeyVaultBlobStorageProvider(Uri vaultUri, string azureAadClientId, string azureAadClientSecret)
      {
         _credential = new ClientCredential(azureAadClientId, azureAadClientSecret);

         _vaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(GetAccessTokenAsync), GetHttpClient());

         _vaultUri = vaultUri.ToString().Trim('/');
      }

      #region [ IBlobStorage ]

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         if (!StoragePath.IsRootPath(options.FolderPath)) return new List<Blob>();

         var secretNames = new List<Blob>();
         IPage<SecretItem> page = await _vaultClient.GetSecretsAsync(_vaultUri).ConfigureAwait(false);

         do
         {
            var ids = page
               .Select((Func<SecretItem, Blob>)AzureKeyVaultBlobStorageProvider.ToBlobId)
               .Where(options.IsMatch)
               .Where(s => options.BrowseFilter == null || options.BrowseFilter(s))
               .ToList();
            secretNames.AddRange(ids);

            if(options.MaxResults != null && secretNames.Count >= options.MaxResults.Value)
            {
               return secretNames.Take(options.MaxResults.Value).ToList();
            }
         }
         while (page.NextPageLink != null && (page = await _vaultClient.GetSecretsNextAsync(page.NextPageLink).ConfigureAwait(false)) != null);

         return secretNames;
      }

      private static Blob ToBlobId(SecretItem item)
      {
         int idx = item.Id.LastIndexOf('/');
         var blob = new Blob(item.Id.Substring(idx + 1), BlobItemKind.File);

         blob.LastModificationTime = item.Attributes.Updated;

         if(item.Attributes.Created != null)
            blob.Properties["Created"] = item.Attributes.Created.Value.ToIso8601DateString();
         if(item.Attributes.Enabled != null)
            blob.Properties["Enabled"] = item.Attributes.Enabled.Value.ToString();
         if(item.Attributes.Expires != null)
            blob.Properties["Expires"] = item.Attributes.Expires.Value.ToIso8601DateString();
         if(item.Attributes.NotBefore != null)
            blob.Properties["NotBefore"] = item.Attributes.NotBefore.Value.ToIso8601DateString();
         if(item.Attributes.RecoveryLevel != null)
            blob.Properties["RecoveryLevel"] = item.Attributes.RecoveryLevel;

         if(item.ContentType != null)
            blob.Properties["ContentType"] = item.ContentType;
         if(item.Managed != null)
            blob.Properties["Managed"] = item.Managed.Value.ToString();

         if(item.Tags != null && item.Tags.Count > 0)
            blob.Metadata.MergeRange(item.Tags);

         return blob;
      }

      public Task<Stream> OpenWriteAsync(string fullPath, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         ValidateSecretName(fullPath);
         if (append) throw new ArgumentException("appending to secrets is not supported", nameof(append));

         var callbackStream = new FixedStream(new MemoryStream(), null, async fx =>
         {
            string value = Encoding.UTF8.GetString(((MemoryStream)fx.Parent).ToArray());

            await _vaultClient.SetSecretAsync(_vaultUri, fullPath, value).ConfigureAwait(false);
         });

         return Task.FromResult<Stream>(callbackStream);
         
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         ValidateSecretName(fullPath);

         SecretBundle secret;
         try
         {
            secret = await _vaultClient.GetSecretAsync(_vaultUri, fullPath).ConfigureAwait(false);
         }
         catch(KeyVaultErrorException ex)
         {
            if (IsNotFound(ex)) return null;
            TryHandleException(ex);
            throw;
         }

         string value = secret.Value;

         return value.ToMemoryStream();
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         await Task.WhenAll(fullPaths.Select(fullPath => _vaultClient.DeleteSecretAsync(_vaultUri, fullPath))).ConfigureAwait(false);
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         return await Task.WhenAll(fullPaths.Select(fullPath => ExistsAsync(fullPath))).ConfigureAwait(false);
      }

      private async Task<bool> ExistsAsync(string fullPath)
      {
         SecretBundle secret;

         try
         {
            secret = await _vaultClient.GetSecretAsync(_vaultUri, fullPath).ConfigureAwait(false);
         }
         catch (KeyVaultErrorException)
         {
            secret = null;
         }

         return secret != null;
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
            SecretBundle secret = await _vaultClient.GetSecretAsync(_vaultUri, fullPath).ConfigureAwait(false);
            byte[] data = Encoding.UTF8.GetBytes(secret.Value);
            return new Blob(fullPath)
            {
               Size = data.Length,
               MD5 = secret.Value.GetHash(HashType.Md5),
               LastModificationTime = secret.Attributes.Updated
            };
         }
         catch(KeyVaultErrorException ex) when(ex.Response.StatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      #endregion

      /// <summary>
      /// Gets the access token
      /// </summary>
      /// <param name="authority"> Authority </param>
      /// <param name="resource"> Resource </param>
      /// <param name="scope"> scope </param>
      /// <returns> token </returns>
      public async Task<string> GetAccessTokenAsync(string authority, string resource, string scope)
      {
         var context = new AuthenticationContext(authority, TokenCache.DefaultShared);

         AuthenticationResult result = await context.AcquireTokenAsync(resource, _credential).ConfigureAwait(false);

         return result.AccessToken;
      }

      /// <summary>
      /// Create an HttpClient object that optionally includes logic to override the HOST header
      /// field for advanced testing purposes.
      /// </summary>
      /// <returns>HttpClient instance to use for Key Vault service communication</returns>
      private HttpClient GetHttpClient()
      {
         return new HttpClient();
      }

      private static void TryHandleException(KeyVaultErrorException ex)
      {
         if(IsNotFound(ex))
         {
            throw new StorageException(ErrorCode.NotFound, ex);
         }
      }

      private static bool IsNotFound(KeyVaultErrorException ex)
      {
         return ex.Body.Error.Code == "SecretNotFound";
      }

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


   }
}
