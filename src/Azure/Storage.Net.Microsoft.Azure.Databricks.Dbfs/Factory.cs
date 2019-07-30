using System;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Databricks.Dbfs;

namespace Storage.Net
{
   /// <summary>
   /// Library Factory
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Use this module
      /// </summary>
      /// <param name="factory"></param>
      /// <returns></returns>
      public static IModulesFactory UseAzureDatabricksDbfsStorage(this IModulesFactory factory)
      {
         return factory.Use(new Module());
      }

      /// <summary>
      /// Create Azure Databricks DBFS storage
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="baseUri"></param>
      /// <param name="token"></param>
      /// <param name="isReadOnly">When true, all the write operations will throw <see cref="InvalidOperationException"/></param>
      /// <returns></returns>
      public static IBlobStorage AzureDatabricksDbfs(this IBlobStorageFactory factory,
         string baseUri,
         string token,
         bool isReadOnly)
      {
         return new AzureDatabricksDbfsBlobStorage(baseUri, token, isReadOnly);
      }
   }
}
