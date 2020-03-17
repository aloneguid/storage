using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   class WorkspaceStorage : GenericBlobStorage
   {
      private readonly IWorkspaceApi _api;

      public WorkspaceStorage(IWorkspaceApi api)
      {
         _api = api;
      }

      protected override bool CanListHierarchy => false;

      protected override async Task<IReadOnlyCollection<Blob>> ListAtAsync(string path, ListOptions options, CancellationToken cancellationToken)
      {
         IEnumerable<ObjectInfo> objects = await _api.List(StoragePath.Normalize(path, true)).ConfigureAwait(false);

         return objects.Select(ToBlob).ToList();
      }

      public override async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         if(fullPath is null)
            throw new ArgumentNullException(nameof(fullPath));

         //parse optional format
         ExportFormat exportFormat = ExportFormat.SOURCE;
         int hashIdx = fullPath.LastIndexOf("#");
         if(hashIdx != -1)
         {
            string formatName = fullPath.Substring(hashIdx + 1);
            fullPath = fullPath.Substring(0, hashIdx);
            if(Enum.TryParse(formatName, true, out ExportFormat ef))
            {
               exportFormat = ef;
            }
         }

         byte[] notebookBytes = await _api.Export(StoragePath.Normalize(fullPath, true), exportFormat);
         return new MemoryStream(notebookBytes);
      }

      private Blob ToBlob(ObjectInfo oi)
      {
         var blob = new Blob(oi.Path, oi.ObjectType == ObjectType.DIRECTORY ? BlobItemKind.Folder : BlobItemKind.File);
         blob.TryAddProperties(
            "ObjectId", oi.ObjectId,
            "ObjectType", oi.ObjectType,
            "Language", oi.Language);
         return blob;
      }
   }
}
