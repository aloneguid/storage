namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class AclItem
   {
      public string User { get; set; }
      public AclPermission Access { get; set; }
      public AclPermission Default { get; set; }
   }
}
