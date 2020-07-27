using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Storage.Net.Blobs;
using Objects = Google.Apis.Storage.v1.Data.Objects;
using Object = Google.Apis.Storage.v1.Data.Object;
using Google;
using System.Net;
using System.Linq;
using Google.Apis.Storage.v1;

namespace Storage.Net.Gcp.CloudStorage.Blobs
{
   class GoogleCloudStorageBlobStorage : GenericBlobStorage
   {
      //for intro see https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-csharp

      private readonly StorageClient _client;
      private readonly string _bucketName;

      protected override bool CanListHierarchy => false;

      public GoogleCloudStorageBlobStorage(string bucketName, GoogleCredential credential = null, EncryptionKey encryptionKey = null) : base()
      {
         _client = StorageClient.Create(credential, encryptionKey);
         _bucketName = bucketName;
      }

      protected override async Task<IReadOnlyCollection<Blob>> ListAtAsync(string path, ListOptions options, CancellationToken cancellationToken)
      {
         ObjectsResource.ListRequest request = _client.Service.Objects.List(_bucketName);
         request.Prefix = StoragePath.IsRootPath(path) ? null : (NormalisePath(path) + "/");
         request.Delimiter = "/";

         var page = new List<Blob>();
         do
         {
            Objects serviceObjects = await request.ExecuteAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            if(serviceObjects.Items != null)
            {
               page.AddRange(GConvert.ToBlobs(serviceObjects.Items, options));
            }

            if(serviceObjects.Prefixes != null)
            {
               //the only info we have about prefixes is it's name
               page.AddRange(serviceObjects.Prefixes.Select(p => new Blob(p, BlobItemKind.Folder)));
            }


            request.PageToken = serviceObjects.NextPageToken;
         }
         while(request.PageToken != null);

         return page;
      }


      private async Task<IReadOnlyCollection<Blob>> LegacyListAtAsync(string path, ListOptions options, CancellationToken cancellationToken)
      {
         PagedAsyncEnumerable<Objects, Object> objects = _client.ListObjectsAsync(
            _bucketName,
            StoragePath.IsRootPath(options.FolderPath) ? null : options.FolderPath,
            new ListObjectsOptions
            {
               Delimiter = options.Recurse ? null : "/"
            });

         return await GConvert.ToBlobsAsync(objects, options).ConfigureAwait(false);
      }

      public override async Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(blobs);

         await Task.WhenAll(blobs.Select(b => SetBlobAsync(b, cancellationToken))).ConfigureAwait(false);
      }

      private async Task SetBlobAsync(Blob blob, CancellationToken cancellationToken = default)
      {
         Object item = await _client.GetObjectAsync(_bucketName, NormalisePath(blob.FullPath), cancellationToken: cancellationToken).ConfigureAwait(false);

         if(item.Metadata == null)
         {
            item.Metadata = new Dictionary<string, string>();
         }

         foreach(KeyValuePair<string, string> metadata in blob.Metadata)
         {
            if(item.Metadata.ContainsKey(metadata.Key))
            {
               item.Metadata[metadata.Key] = metadata.Value;
            }
            else
            {
               item.Metadata.Add(metadata.Key, metadata.Value);
            }
         }

         await _client.UpdateObjectAsync(item, cancellationToken: cancellationToken).ConfigureAwait(false);
      }

      protected override async Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken)
      {
         fullPath = NormalisePath(fullPath);

         try
         {
            Object obj = await _client.GetObjectAsync(_bucketName, fullPath,
               new GetObjectOptions
               {
                  //todo
               },
               cancellationToken).ConfigureAwait(false);

            return GConvert.ToBlob(obj);
         }
         catch(GoogleApiException ex) when(ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
      }

      protected override async Task DeleteSingleAsync(string fullPath, CancellationToken cancellationToken)
      {
         try
         {
            await _client.DeleteObjectAsync(_bucketName, NormalisePath(fullPath), cancellationToken: cancellationToken).ConfigureAwait(false);
         }
         catch(GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            //when not found, just ignore

            //try delete everything recursively
            IReadOnlyCollection<Blob> childObjects = await ListAsync(new ListOptions { Recurse = true }, cancellationToken).ConfigureAwait(false);
            foreach(Blob blob in childObjects)
            {
               try
               {
                  await _client.DeleteObjectAsync(_bucketName, NormalisePath(blob.FullPath), cancellationToken: cancellationToken).ConfigureAwait(false);
               }
               catch(GoogleApiException exc) when(exc.HttpStatusCode == HttpStatusCode.NotFound)
               {

               }
            }
         }
      }

      protected override async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         try
         {
            await _client.GetObjectAsync(
               _bucketName, NormalisePath(fullPath),
               null,
               cancellationToken).ConfigureAwait(false);

            return true;
         }
         catch(GoogleApiException ex) when(ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            return false;
         }
      }


      public override async Task WriteAsync(string fullPath, Stream dataStream,
         bool append = false, CancellationToken cancellationToken = default)
      {
         if(append)
            throw new NotSupportedException();
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = NormalisePath(fullPath);

         await _client.UploadObjectAsync(_bucketName, fullPath, null, dataStream, cancellationToken: cancellationToken).ConfigureAwait(false);
      }

      public override async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = NormalisePath(fullPath);

         // no read streaming support in this crappy SDK

         var ms = new MemoryStream();
         try
         {
            await _client.DownloadObjectAsync(_bucketName, fullPath, ms, cancellationToken: cancellationToken).ConfigureAwait(false);
         }
         catch(GoogleApiException ex) when(ex.HttpStatusCode == HttpStatusCode.NotFound)
         {
            return null;
         }
         ms.Position = 0;
         return ms;
      }

      /// <summary>
      /// GCP requires no trailing root
      /// </summary>
      private static string NormalisePath(string path)
      {
         path = StoragePath.Normalize(path);
         return path.Substring(1);
      }
   }
}
