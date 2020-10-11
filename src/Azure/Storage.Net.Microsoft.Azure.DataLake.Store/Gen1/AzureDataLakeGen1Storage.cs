using Microsoft.Rest.Azure.Authentication;
using Storage.Net.Blobs;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.Acl;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen1
{
   class AzureDataLakeGen1Storage : IAzureDataLakeGen1BlobStorage, IExtendedBlobStorage
   {
      private readonly string _accountName;
      private readonly string _domain;
      private readonly string _clientId;
      private readonly string _clientSecret;
      private ServiceClientCredentials _credential;
      private AdlsClient _client;

      //some info on how to use sdk here: https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-net-sdk

      private AzureDataLakeGen1Storage(string accountName, string domain, string clientId, string clientSecret, string clientCert)
      {
         _accountName = accountName ?? throw new ArgumentNullException(nameof(accountName));

         _domain = domain ?? throw new ArgumentNullException(nameof(domain));
         _clientId = clientId;
         _clientSecret = clientSecret;
      }

      /// <summary>
      /// Returns the actual credential object used to authenticate to ADLS. Note that this will only be populated
      /// once you make at least one successful call.
      /// </summary>
      public ServiceClientCredentials Credentials => _credential;

      public int ListBatchSize { get; set; } = 5000;

      public static AzureDataLakeGen1Storage CreateByClientSecret(string accountName, string tenantId, string principalId, string principalSecret)
      {
         return CreateByClientSecret(accountName, new NetworkCredential(principalId, principalSecret, tenantId));
      }

      public static AzureDataLakeGen1Storage CreateByClientSecret(string accountName, NetworkCredential credential)
      {
         if (credential == null) throw new ArgumentNullException(nameof(credential));

         if (string.IsNullOrEmpty(credential.Domain))
            throw new ArgumentException("Tenant ID (Domain in NetworkCredential) part is required");

         if (string.IsNullOrEmpty(credential.UserName))
            throw new ArgumentException("Principal ID (Username in NetworkCredential) part is required");

         if (string.IsNullOrEmpty(credential.Password))
            throw new ArgumentException("Principal Secret (Password in NetworkCredential) part is required");

         return new AzureDataLakeGen1Storage(accountName, credential.Domain, credential.UserName, credential.Password, null);
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         var browser = new DirectoryBrowser(client, ListBatchSize);
         return await browser.BrowseAsync(options, cancellationToken).ConfigureAwait(false);
      }

      public async Task WriteAsync(string fullPath, Stream dataStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         if(dataStream is null)
            throw new ArgumentNullException(nameof(dataStream));

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         AdlsOutputStream stream;

         if(append)
         {
            stream = await client.GetAppendStreamAsync(fullPath, cancellationToken).ConfigureAwait(false);
         }
         else
         {
            stream = await client.CreateFileAsync(fullPath, IfExists.Overwrite,
               createParent: true,
               cancelToken: cancellationToken).ConfigureAwait(false);
         }

         using(stream)
         {
            await dataStream.CopyToAsync(stream).ConfigureAwait(false);
         }
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         try
         {
            AdlsInputStream response = await client.GetReadStreamAsync(fullPath, cancellationToken).ConfigureAwait(false);

            return response;
         }
         catch (AdlsException ex) when (ex.HttpStatus == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         //DeleteRecursiveAsync works with both files and folder, however it will delete folders recursively (that's what I want)
         await Task.WhenAll(
            fullPaths.Select(
               fullPath => client.DeleteRecursiveAsync(fullPath, cancellationToken))).ConfigureAwait(false);
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         var result = new List<bool>();

         foreach (string fullPath in fullPaths)
         {
            bool exists = client.CheckExists(fullPath);

            result.Add(exists);
         }

         return result;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         return await Task.WhenAll(fullPaths.Select(fullPath => GetBlobWithMetaAsync(fullPath, client, cancellationToken))).ConfigureAwait(false);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException("ADLS Gen1 doesn't support file metadata");
      }

      private async Task<Blob> GetBlobWithMetaAsync(string fullPath, AdlsClient client, CancellationToken cancellationToken)
      {
         try
         {
            DirectoryEntry entry = await client.GetDirectoryEntryAsync(fullPath, cancelToken: cancellationToken).ConfigureAwait(false);
            return new Blob(fullPath)
            {
               Size = entry.Length,
               LastModificationTime = entry.LastModifiedTime
            };
         }
         catch(AdlsException ex) when (ex.HttpStatus == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      #region [ Security ]

      public async Task GetAccessControlAsync(string fullPath)
      {
         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         AclStatus acl = await client.GetAclStatusAsync(fullPath, UserGroupRepresentation.ObjectID).ConfigureAwait(false);
      }

      #endregion

      private async Task<AdlsClient> GetAdlsClientAsync()
      {
         if (_client != null) return _client;

         ServiceClientCredentials creds = await GetCredsAsync().ConfigureAwait(false);

         _client = AdlsClient.CreateClient($"{_accountName}.azuredatalakestore.net", creds);

         return _client;
      }

      private async Task<ServiceClientCredentials> GetCredsAsync()
      {
         if (_credential != null) return _credential;

         if (_clientSecret != null)
         {
            var cc = new ClientCredential(_clientId, _clientSecret);
            _credential = await ApplicationTokenProvider.LoginSilentAsync(_domain, cc).ConfigureAwait(false);
         }
         else
         {
            //var ac = new ClientAssertionCertificate(_clientSecret, )
            //await ApplicationTokenProvider.LoginSilentWithCertificateAsync(_domain, )
            throw new NotImplementedException();
         }

         return _credential;
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      #region [ IExtendedBlobStorage ]

      public async Task RenameAsync(string oldPath, string newPath, CancellationToken cancellationToken = default)
      {
         AdlsClient client = await GetAdlsClientAsync().ConfigureAwait(false);

         await client.RenameAsync(StoragePath.Normalize(oldPath), StoragePath.Normalize(newPath), true, cancellationToken).ConfigureAwait(false);
      }

      #endregion
   }
}
