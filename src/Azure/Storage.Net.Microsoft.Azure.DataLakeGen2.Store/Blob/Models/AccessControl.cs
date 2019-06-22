using System.Collections.Generic;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class AccessControl
   {
      public List<AclItem> Acl { get; set; }
      public string Group { get; set; }
      public string Owner { get; set; }
      public string Permissions { get; set; }
   }
}
