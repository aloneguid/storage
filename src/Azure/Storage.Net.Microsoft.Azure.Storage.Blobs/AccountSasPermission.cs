using System;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Specifies the set of possible permissions for a shared access account policy.
   /// </summary>
   [Flags]
   public enum AccountSasPermission
   {
      /// <summary>
      /// Indicates that Read is permitted.
      /// </summary>
      Read = 1,

      /// <summary>
      /// Indicates that Write is permitted.
      /// </summary>
      Write = 2,

      /// <summary>
      /// Indicates that Delete is permitted.
      /// </summary>
      Delete = 4,

      /// <summary>
      /// Indicates that List is permitted.
      /// </summary>
      List = 8,

      /// <summary>
      /// Indicates that Add is permitted.
      /// </summary>
      Add = 16,

      /// <summary>
      /// Indicates that Create is permitted.
      /// </summary>
      Create = 32,

      /// <summary>
      /// Indicates that Update is permitted.
      /// </summary>
      Update = 64,

      /// <summary>
      /// Indicates that Delete is permitted.
      /// </summary>
      Process = 128,

      /// <summary>
      /// Indicates that all permissions are set.
      /// </summary>
      All = ~0
   }
}
