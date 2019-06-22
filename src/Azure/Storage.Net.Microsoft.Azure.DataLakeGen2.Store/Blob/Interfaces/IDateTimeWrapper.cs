using System;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces
{
   public interface IDateTimeWrapper
   {
      DateTime Now { get; }
   }
}