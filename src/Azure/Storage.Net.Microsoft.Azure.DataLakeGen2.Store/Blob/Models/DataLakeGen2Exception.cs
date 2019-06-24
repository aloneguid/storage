using System;
using System.Net;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class DataLakeGen2Exception : Exception
   {
      public DataLakeGen2Exception(string message, Exception innerException) : base(message, innerException)
      {

      }

      public HttpStatusCode StatusCode { get; set; }
   }
}
