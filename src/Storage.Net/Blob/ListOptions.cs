using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Blob
{
   /// <summary>
   /// Options for listing storage content
   /// </summary>
   public class ListOptions
   {
      private string _prefix;

      /// <summary>
      /// Folder path to start browsing from. When not set scanning starts from the root folder.
      /// </summary>
      public string FolderPath { get; set; }

      /// <summary>
      /// Gets or sets a browsing filter used by some implementations which can filter out results before returning it to you.
      /// This is useful to minimise amount of RAM taken when returning the results and then filtering them on client side.
      /// Note that filtering will be happening on the client side, therefore this is the least efficient filter and should
      /// only be used when you're concerned about RAM usage.
      /// </summary>
      public Func<BlobId, bool> BrowseFilter { get; set; }

      /// <summary>
      /// Prefix to filter file name by. Folders are not affected by this filter. If you list files recursively
      /// the prefix is applied in every folder.
      /// </summary>
      public string FilePrefix
      {
         get => _prefix;
         set
         {
            GenericValidation.CheckBlobPrefix(value);
            _prefix = value;
         }
      }

      /// <summary>
      /// When true, operation will recursively navigate down the folders.
      /// </summary>
      public bool Recurse { get; set; }

      /// <summary>
      /// When set, limits the maximum amount of results. The count affects all object counts, including files and folders.
      /// </summary>
      public int? MaxResults { get; set; }

      /// <summary>
      /// When set, includes blob metadata in the response if the provider supports it. False by default
      /// only because metadata consumes more memory in response object, although most of the providers do not
      /// have any overhead in creating this metadata.
      /// </summary>
      public bool IncludeMetaWhenKnown { get; set; } = false;

      /// <summary>
      /// When set, an implementing provider might be able to send notification on progress callback if it supports them.
      /// First parameter indicates number of items already listed, second - total number of items (if known).
      /// </summary>
      public Action<long, long> ListProgressCallback { get; set; }

      /// <summary>
      /// When supported, sets the maximum number of threads to be used for listing operations
      /// </summary>
      public int MaxDegreeOfParalellism { get; set; } = 10;

      /// <summary>
      /// Helper method that returns true if a <see cref="BlobId"/> matches these list options.
      /// </summary>
      public bool IsMatch(BlobId id)
      {
         return _prefix == null || id.Kind != BlobItemKind.File || id.Id.StartsWith(_prefix);
      }

      /// <summary>
      /// Only for internal use
      /// </summary>
      public bool Add(ICollection<BlobId> dest, ICollection<BlobId> src)
      {
         if(MaxResults == null || (dest.Count + src.Count < MaxResults.Value))
         {
            dest.AddRange(src);
            return false;
         }

         dest.AddRange(src.Take(MaxResults.Value - dest.Count));
         return true;
      }

      /// <summary>
      /// Clones list options
      /// </summary>
      /// <returns></returns>
      public ListOptions Clone()
      {
         return (ListOptions)(MemberwiseClone());
      }
   }
}
