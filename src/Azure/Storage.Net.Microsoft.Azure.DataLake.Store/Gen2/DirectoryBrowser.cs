using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   class DirectoryBrowser
   {
      private readonly DataLakeGen2Client _client;

      public DirectoryBrowser(DataLakeGen2Client client)
      {
         _client = client;
      }
   }
}
