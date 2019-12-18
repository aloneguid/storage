namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Blob container public access type
   /// </summary>
   public enum ContainerPublicAccessType
   {
      /// <summary>
      /// No public access. Only the account owner can read resources in this container.
      /// </summary>
      Off,

      /// <summary>
      /// Container-level public access. Anonymous clients can read container and blob data;
      /// </summary>
      Container,


      /// <summary>
      /// Blob-level public access. Anonymous clients can read blob data within this
      ///  container, but not container data.
      /// </summary>
      Blob
   }
}
