using System;
using System.Collections.Generic;
using System.Linq;

namespace Storage.Net
{
   /// <summary>
   /// Storage Path utilities
   /// </summary>
   public static class StoragePath
   {
      /// <summary>
      /// Character used to split paths 
      /// </summary>
      public const char PathSeparator = '/';

      /// <summary>
      /// Character used to split paths as a string value
      /// </summary>
      public static readonly string PathSeparatorString = new string(PathSeparator, 1);

      /// <summary>
      /// Character used to split paths 
      /// </summary>
      public static readonly string PathStrSeparator = new string(PathSeparator, 1);

      /// <summary>
      /// Returns '/'
      /// </summary>
      public static readonly string RootFolderPath = "/";

      /// <summary>
      /// Combines parts of path
      /// </summary>
      /// <param name="parts"></param>
      /// <returns></returns>
      public static string Combine(IEnumerable<string> parts)
      {
         if (parts == null) return Normalize(null);

         return Normalize(string.Join(PathStrSeparator, parts.Where(p => p != null).Select(p => NormalizePart(p))));
      }

      /// <summary>
      /// Gets parent path of this item
      /// </summary>
      public static string GetParent(string path)
      {
         if (path == null) return null;

         string[] parts = Split(path);
         if (parts.Length == 0) return null;

         return parts.Length > 1
            ? Combine(parts.Take(parts.Length - 1))
            : PathStrSeparator;
      }

      /// <summary>
      /// Combines parts of path
      /// </summary>
      /// <param name="parts"></param>
      /// <returns></returns>
      public static string Combine(params string[] parts)
      {
         return Combine((IEnumerable<string>)parts);
      }

      /// <summary>
      /// Normalizes path. Normalisation makes sure that:
      /// - When path is null returns root path '/'
      /// - path separators are trimmed from both ends
      /// </summary>
      /// <param name="path"></param>
      /// <param name="includeTrailingRoot">When true, includes trailing '/' as path prefix</param>
      public static string Normalize(string path, bool includeTrailingRoot = false)
      {
         if (path == null) return RootFolderPath;

         path = path.Trim(PathSeparator);

         return includeTrailingRoot ?
            PathSeparatorString + path
            : path;
      }

      /// <summary>
      /// Normalizes path part
      /// </summary>
      /// <param name="part"></param>
      /// <returns></returns>
      public static string NormalizePart(string part)
      {
         if (part == null) throw new ArgumentNullException(nameof(part));

         return part.Trim(PathSeparator);
      }

      /// <summary>
      /// Splits path in parts. Leading and trailing path separators are totally ignored. Note that it returns
      /// null if input path is null.
      /// </summary>
      public static string[] Split(string path)
      {
         if (path == null) return null;

         return path.Split(new[] { PathSeparator }, StringSplitOptions.RemoveEmptyEntries).Select(NormalizePart).ToArray();
      }

      /// <summary>
      /// Checks if path is root folder path, which can be an empty string, null, or the actual root path.
      /// </summary>
      public static bool IsRootPath(string path)
      {
         return string.IsNullOrEmpty(path) || path == RootFolderPath;
      }

      /// <summary>
      /// Compare that two path entries are equal. This takes into account path entries which are slightly different as strings but identical in physical location.
      /// </summary>
      public static bool ComparePath(string path1, string path2)
      {
         return Normalize(path1) == Normalize(path2);
      }
   }
}
