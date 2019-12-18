using System;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Specifies possible permissions flags for container access policy
   /// </summary>
   [Flags]
   public enum ContainerSasPermission
   {
      /// <summary>
      /// Indicates that Read is permitted.
      /// </summary>
      Read = 1,

      /// <summary>
      /// Indicates that Add is permitted.
      /// </summary>
      Add = 2,

      /// <summary>
      /// Indicates that Create is permitted.
      /// </summary>
      Create = 4,

      /// <summary>
      /// Indicates that Write is permitted.
      /// </summary>
      Write = 8,

      /// <summary>
      /// Indicates that Delete is permitted.
      /// </summary>
      Delete = 16,

      /// <summary>
      /// Indicates that List is permitted.
      /// </summary>
      List = 32,

      /// <summary>
      /// Indicates that all permissions are set.
      /// </summary>
      All = ~0
   }
}
