using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using NetBox.Extensions;
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

         return objects.Select(ToBlob).Where(b => b != null).ToList();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="fullPath"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      /// <remarks>
      /// 
      /// </remarks>
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

         //notebooks are passed with extensions at the end (.py, .scala etc.) so you need to remove them first
         string path = Path.ChangeExtension(fullPath, null);   // removes extension

         byte[] notebookBytes = await _api.Export(StoragePath.Normalize(path, true), exportFormat);
         return new MemoryStream(notebookBytes);
      }

      public override async Task WriteAsync(
         string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default)
      {
         string path = StoragePath.Normalize(fullPath, true);

         //import will fail unless all the parent folders exist, so make sure they do
         string parent = StoragePath.GetParent(path);
         if(!StoragePath.IsRootPath(parent))
         {
            await _api.Mkdirs(StoragePath.Normalize(parent, true));
         }

         GetImportParameters(path, out ExportFormat exportFormat, out Language? language);

         await _api.Import(path, exportFormat, language, dataStream.ToByteArray(), true);
      }

      private Blob ToBlob(ObjectInfo oi)
      {
         if(oi.ObjectType == ObjectType.DIRECTORY)
         {
            var blob = new Blob(oi.Path, BlobItemKind.Folder);
            blob.TryAddProperties(
               "ObjectId", oi.ObjectId,
               "ObjectType", oi.ObjectType);
            return blob;
         }
         else if(oi.ObjectType == ObjectType.NOTEBOOK)
         {
            string path = oi.Path;

            switch(oi.Language.Value)
            {
               case Language.PYTHON:
                  path += ".py";
                  break;
               case Language.SCALA:
                  path += ".scala";
                  break;
               case Language.R:
                  path += ".r";
                  break;
               case Language.SQL:
                  path += ".sql";
                  break;
            }

            var blob = new Blob(path, BlobItemKind.File);
            blob.TryAddProperties(
               "ObjectId", oi.ObjectId,
               "ObjectType", oi.ObjectType,
               "Language", oi.Language.Value);
            return blob;
         }

         return null;
      }

      private static void GetImportParameters(string path, out ExportFormat exportFormat, out Language? language)
      {
         string ext = Path.GetExtension(path);
         switch(ext.ToLower())
         {
            case ".ipynb":
               exportFormat = ExportFormat.JUPYTER;
               language = null;
               break;
            case ".html":
               exportFormat = ExportFormat.HTML;
               language = null;
               break;
            case ".dbc":
               exportFormat = ExportFormat.DBC;
               language = null;
               break;
            case ".py":
               exportFormat = ExportFormat.SOURCE;
               language = Language.PYTHON;
               break;
            case ".scala":
               exportFormat = ExportFormat.SOURCE;
               language = Language.SCALA;
               break;
            case ".sql":
               exportFormat = ExportFormat.SOURCE;
               language = Language.SQL;
               break;
            case ".r":
               exportFormat = ExportFormat.SOURCE;
               language = Language.R;
               break;

            default:
               throw new ArgumentException($"can't figure out format/language for '{path}'", nameof(path));
         }
      }
   }
}
