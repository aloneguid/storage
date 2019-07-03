using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Storage.Net.Blobs;

namespace Storage.Net
{
   /// <summary>
   /// A collection of generic library wise validations
   /// </summary>
   public static class GenericValidation
   {
      private const int MaxBlobPrefixLength = 50;

      /// <summary>
      /// Validates blob prefix search
      /// </summary>
      /// <param name="prefix"></param>
      public static void CheckBlobPrefix(string prefix)
      {
         if (prefix == null) return;

         string[] parts = prefix.Split('/');

         foreach (string part in parts)
         {
            if (part.Length > MaxBlobPrefixLength)
               throw new ArgumentException(
                  string.Format("blob prefix cannot exceed {0} characters", MaxBlobPrefixLength),
                  nameof(prefix));
         }
      }

      /// <summary>
      /// Validates blob full path
      /// </summary>
      /// <param name="fullPath"></param>
      public static void CheckBlobFullPath(string fullPath)
      {
         if (fullPath == null) throw new ArgumentNullException(nameof(fullPath));
      }

      /// <summary>
      /// Checks blob full path for generic rules
      /// </summary>
      public static void CheckBlobFullPaths(IEnumerable<string> fullPaths)
      {
         if (fullPaths == null) return;

         foreach (string fullPath in fullPaths)
         {
            CheckBlobFullPath(fullPath);
         }
      }

      /// <summary>
      /// Checks blob full path for generic rules
      /// </summary>
      public static void CheckBlobFullPaths(IEnumerable<Blob> blobs)
      {
         if(blobs == null)
            return;

         CheckBlobFullPaths(blobs.Select(b => b.FullPath));
      }

      /// <summary>
      /// Checks source stream for generic rules
      /// </summary>
      public static void CheckSourceStream(Stream inputStream)
      {
         if (inputStream == null) throw new ArgumentNullException(nameof(inputStream));

         try
         {
            long l = inputStream.Length;
         }
         catch(NotSupportedException ex)
         {
            throw new ArgumentException("stream must support getting a length", nameof(inputStream), ex);
         }

      }
   }
}
