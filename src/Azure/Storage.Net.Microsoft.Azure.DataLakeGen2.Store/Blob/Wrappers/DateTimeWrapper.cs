using System;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Wrappers
{
   public class DateTimeWrapper : IDateTimeWrapper
   {
      public DateTime Now => DateTime.Now;
   }
}