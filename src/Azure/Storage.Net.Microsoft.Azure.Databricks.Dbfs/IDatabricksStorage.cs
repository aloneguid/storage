using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   /// <summary>
   /// Databricks specific functionality
   /// </summary>
   public interface IDatabricksStorage : IBlobStorage
   {
      /// <summary>
      /// Gets native reference. Do not use as there is no guarantee I won't switch the library in future.
      /// </summary>
      /// <returns></returns>
      DatabricksClient GetMicrosoftAzureDatabrickClientLibraryClient();
   }
}
