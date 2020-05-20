using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NetBox.Extensions;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Allows to combine several storage providers (or even of the same type) in one virtual storage interface.
   /// Providers are distinguished using a prefix. Essentially this allows to mount providers in a virtual filesystem.
   /// </summary>
   public class VirtualStorage : IVirtualStorage
   {
      private readonly ConcurrentDictionary<string, HashSet<Blob>> _pathToMountBlobs = new ConcurrentDictionary<string, HashSet<Blob>>();
      private readonly List<Blob> _mountPoints = new List<Blob>();

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

         _mountPoints.Add(new Blob(path) { Tag = storage });

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

      class MpTag : MpTag<object>
      {
      }

      class MpTag<T>
      {
         public string fullPath;
         public string relPath;
         public T result;
      }

      private Dictionary<IBlobStorage, List<MpTag<T>>> Explode<T>(
         IEnumerable<string> fullPaths,
         out Dictionary<string, MpTag<T>> fullPathToTag)
      {
         var dic1 = new Dictionary<IBlobStorage, List<MpTag<T>>>();
         fullPathToTag = new Dictionary<string, MpTag<T>>();
         //var fullToRel = new Dictionary<string, string>();

         foreach(string fp in fullPaths)
         {
            if(!TryExplodeToMountPoint(fp, out IBlobStorage storage, out string relPath))
            {
               fullPathToTag[fp] = null;
            }
            else
            {
               if(!dic1.TryGetValue(storage, out List<MpTag<T>> tags))
               {
                  tags = new List<MpTag<T>>();
                  dic1[storage] = tags;
               }

               var tag = new MpTag<T> { fullPath = fp, relPath = relPath };

               tags.Add(tag);
               fullPathToTag[fp] = tag;
            }
         }
         return dic1;
      }

      private Dictionary<IBlobStorage, List<string>> Explode(IEnumerable<string> fullPaths)
      {
         var map = new Dictionary<IBlobStorage, List<string>>();

         foreach(string fp in fullPaths)
         {
            if(TryExplodeToMountPoint(fp, out IBlobStorage storage, out string relPath))
            { 
               if(!map.TryGetValue(storage, out List<string> relPaths))
               {
                  relPaths = new List<string>();
                  map[storage] = relPaths;
               }

               relPaths.Add(relPath);
            }
         }
         return map;
      }

      private async Task ExecuteAsync(
         IEnumerable<string> fullPaths,
         Func<IBlobStorage, IEnumerable<string>, Task> action)
      {
         Dictionary<IBlobStorage, List<string>> map = Explode(fullPaths);

         IEnumerable<Task> tasks = map.Select(pair => action(pair.Key, pair.Value));

         await Task.WhenAll(tasks).ConfigureAwait(false);
      }

      private async Task<IReadOnlyCollection<TResult>> ExecuteAsync<TResult>(
         IEnumerable<string> fullPaths,
         Func<IBlobStorage, IEnumerable<string>, Task<IReadOnlyCollection<TResult>>> action)
      {
         Dictionary<IBlobStorage, List<MpTag<TResult>>> dic = Explode<TResult>(
            fullPaths,
            out Dictionary<string, MpTag<TResult>> fullPathToTag);

         // execute and assign result
         foreach(KeyValuePair<IBlobStorage, List<MpTag<TResult>>> pair in dic)
         {
            IEnumerable<string> rps = pair.Value.Select(v => v.relPath);

            IReadOnlyCollection<TResult> br = await action(pair.Key, rps);

            foreach(Tuple<TResult, MpTag<TResult>> doublePair in EnumerableEx.MultiIterate(br, pair.Value))
            {
               doublePair.Item2.result = doublePair.Item1;
            }
         }

         // collect full result
         return fullPaths.Select(fp => fullPathToTag[fp].result).ToList();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="fullPaths"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public virtual Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return ExecuteAsync(fullPaths, (storage, paths) => storage.DeleteAsync(paths, cancellationToken));
      }


      /// <summary>
      /// 
      /// </summary>
      public virtual void Dispose()
      {

      }

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return ExecuteAsync(
            fullPaths,
            (storage, fps) => storage.ExistsAsync(fps, cancellationToken));
      }

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return ExecuteAsync(
            fullPaths,
            (storage, fps) => storage.GetBlobsAsync(fps, cancellationToken));
      }

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

         //find mount points

         List<Blob> mountPoints = _mountPoints.Where(mp => options.FolderPath.StartsWith(mp.FullPath)).ToList();

         foreach(Blob mountPoint in mountPoints)
         {
            IBlobStorage storage = (IBlobStorage)mountPoint.Tag;

            string relPath = options.FolderPath.Substring(mountPoint.FullPath.Length);

            ListOptions mountOptions = options.Clone();
            mountOptions.FolderPath = StoragePath.Normalize(relPath);

            IReadOnlyCollection<Blob> mountResults = await storage.ListAsync(mountOptions, cancellationToken);
            foreach(Blob blob in mountResults)
            {
               blob.PrependPath(mountPoint.FullPath);
            }
            result.AddRange(mountResults);
         }

         return result;
      }

      /// <summary>
      /// 
      /// </summary>
      public virtual async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         if(!TryExplodeToMountPoint(fullPath, out IBlobStorage storage, out string relPath))
            return null;

         return await storage.OpenReadAsync(relPath, cancellationToken).ConfigureAwait(false);
      }

      private bool TryExplodeToMountPoint(string fullPath, out IBlobStorage storage, out string relPath)
      {
         storage = null;
         relPath = null;

         if(fullPath == null)
            return false;

         fullPath = StoragePath.Normalize(fullPath);

         Blob mountPoint = _mountPoints.FirstOrDefault(mp => fullPath.StartsWith(mp.FullPath));
         if(mountPoint == null)
            return false;

         storage = (IBlobStorage)mountPoint.Tag;
         relPath = StoragePath.Normalize(fullPath.Substring(mountPoint.FullPath.Length));
         return true;
      }



      /// <summary>
      /// 
      /// </summary>
      public virtual Task<ITransaction> OpenTransactionAsync() => null;

      /// <summary>
      /// 
      /// </summary>
      public virtual Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// 
      /// </summary>
      public virtual async Task WriteAsync(string fullPath, Stream dataStream, bool append = false, CancellationToken cancellationToken = default)
      {
         if(!TryExplodeToMountPoint(fullPath, out IBlobStorage storage, out string relPath))
            return;


         await storage.WriteAsync(relPath, dataStream, append, cancellationToken).ConfigureAwait(false);
      }
   }
}
