using System.Collections.Generic;
using System.Linq;
using Refit;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest
{
   static class RefitExtensions
   {
      public static string GetHeader<T>(this ApiResponse<T> refitResponse, string name)
      {
         if(!refitResponse.Headers.TryGetValues(name, out IEnumerable<string> values))
            return null;

         return values.First();
      }
   }
}
