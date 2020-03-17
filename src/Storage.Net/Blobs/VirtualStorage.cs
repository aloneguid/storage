using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Allows to combine several storage providers (or even of the same type) in one virtual storage interface.
   /// Providers are distinguished using a prefix. Essentially this allows to mount providers in a virtual filesystem.
   /// </summary>
   public class VirtualStorage : IBlobStorage
   {
      private readonly ConcurrentDictionary<string, HashSet<Blob>> _pathToMountBlobs = new ConcurrentDictionary<string, HashSet<Blob>>();

      /// <summary>
      /// Creates an instance
      /// </summary>
      public VirtualStorage()
      {

      }

      /// <summary>
      /// Mounts a storage to virtual path
      /// </summary>
      /// <param name="path"></param>
      /// <param name="storage"></param>
      public void Mount(string path, IBlobStorage storage)
      {
         if(path is null)
            throw new ArgumentNullException(nameof(path));
         if(storage is null)
            throw new ArgumentNullException(nameof(storage));

         path = StoragePath.Normalize(path);
         string absPath = null;

         string[] parts = StoragePath.Split(path);

         if(parts.Length == 0)   //mount at root
         {
            MountPath(path, storage, true);
         }
         else
         {
            for(int i = 0; i < parts.Length; i++)
            {
               absPath = StoragePath.Combine(absPath, parts[i]);

               MountPath(absPath, storage, i == parts.Length - 1);
            }
         }
      }

      private void MountPath(string path, IBlobStorage storage, bool isMountPoint)
      {
         string containerPath = StoragePath.IsRootPath(path) ? path : StoragePath.GetParent(path);

         if(!_pathToMountBlobs.TryGetValue(containerPath, out HashSet<Blob> blobs))
         {
            blobs = new HashSet<Blob>();
            _pathToMountBlobs[containerPath] = blobs;
         }

         // this is the mount
         if(isMountPoint)
         {
            var mountBlob = new Blob(path, BlobItemKind.Folder) { Tag = storage };
            mountBlob.TryAddProperties("IsMountPoint", true);
            blobs.Add(mountBlob);
         }
         else
         {
            var intBlob = new Blob(path, BlobItemKind.Folder);
            blobs.Add(intBlob);
         }
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="fullPaths"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public virtual Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual void Dispose()
      {

      }

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public async virtual Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         var result = new List<Blob>();

         //mount folders/points
         if(_pathToMountBlobs.TryGetValue(options.FolderPath, out HashSet<Blob> mounts))
         {
            var mountPoints = new List<Blob>();

            foreach(Blob blob in mounts)
            {
               if(blob.Tag == null)
               {
                  result.Add(blob);
               }
               else
               {
                  //mountPoints.Add(blob);

                  if(!StoragePath.IsRootPath(blob.FullPath))
                  {
                     result.Add(blob);
                  }
               }
            }

            foreach(Blob mountPoint in mountPoints)
            {
               IBlobStorage storage = (IBlobStorage)mountPoint.Tag;

               IReadOnlyCollection<Blob> items = await storage.ListAsync(options, cancellationToken);

               result.AddRange(items);
            }
         }

         /*
          * abs path:
          * /f1/f2/f3/f4/f5
          * 
          * mount: /f1
          * list:  /f1/f2/f3/f4
          * 
          * strip mount - /f2/f3/f4 - list from mount
          * 
          * 
          */

         return result;
      }

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<ITransaction> OpenTransactionAsync() => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task WriteAsync(string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default) => throw new NotImplementedException();
   }
}
