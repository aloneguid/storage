namespace Storage.Net.Microsoft.Azure.Storage
{
   internal class Constants
   {
      public const string UseDevelopmentStorageConnectionString = @"UseDevelopmentStorage=true";

      public const string AzureBlobConnectionPrefix = "azure.blob";
      public const string AzureTablesConnectionPrefix = "azure.tables";
      public const string AzureQueueConnectionPrefix = "azure.queue";

      public const string AccountParam = "account";
      public const string KeyParam = "key";
      public const string QueueParam = "queue";
      public const string InvisibilityParam = "invisibility";
      public const string PollParam = "poll";

      public const string UseDevelopmentStorage = "development";
   }
}