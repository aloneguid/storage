using System.Collections.Generic;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Models
{
   class AccessControl
   {
      public List<AclItem> Acl { get; set; }
      public string Group { get; set; }
      public string Owner { get; set; }
      public string Permissions { get; set; }
   }
}
