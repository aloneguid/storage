using System;
using System.Net;
using Config.Net;

namespace Storage.Net.Tests
{
   public interface ITestSettings
   {
      #region [ Azure ]

      [Option(Alias = "Azure.Storage.Name")]
      string AzureStorageName { get; }

      [Option(Alias = "Azure.Storage.Key")]
      string AzureStorageKey { get; }

      [Option(Alias = "Azure.Storage.NativeConnectionString")]
      string AzureStorageNativeConnectionString { get; }

      [Option(Alias = "Azure.DataLakeGen2.Name")]
      string AzureDataLakeGen2Name { get; }

      [Option(Alias = "Azure.DataLakeGen2.Key")]
      string AzureDataLakeGen2Key { get; }

      [Option(Alias = "Azure.DataLakeGen2.TenantId")]
      string AzureDataLakeGen2TenantId { get; }

      [Option(Alias = "Azure.DataLakeGen2.PrincipalId")]
      string AzureDataLakeGen2PrincipalId { get; }

      [Option(Alias = "Azure.DataLakeGen2.PrincipalSecret")]
      string AzureDataLakeGen2PrincipalSecret { get; }

      [Option(Alias = "Azure.DataLakeGen2.TestAdObjectId")]
      string AzureDataLakeGen2TestObjectId { get; }

      [Option(Alias = "Azure.Storage.QueueName", DefaultValue = "local")]
      string AzureStorageQueueName { get; }

      [Option(Alias = "Azure.Storage.ContainerSasUri")]
      Uri AzureContainerSasUri { get; }

      [Option(Alias = "Azure.ServiceBus.ConnectionString")]
      string ServiceBusConnectionString { get; }

      [Option(DefaultValue = "testqueuelocal")]
      string ServiceBusQueueName { get; }

      [Option(DefaultValue = "testtopiclocal")]
      string ServiceBusTopicName { get; }

      [Option(Alias = "Azure.EventHub.ConnectionString")]
      string EventHubConnectionString { get; }

      [Option(Alias = "Azure.DataLake.TenantId")]
      string AzureDataLakeTenantId { get; }

      [Option(Alias = "Azure.DataLake.PrincipalId")]
      string AzureDataLakePrincipalId { get; }

      [Option(Alias = "Azure.DataLake.PrincipalSecret")]
      string AzureDataLakePrincipalSecret { get; }

      [Option(Alias = "Azure.DataLake.Store.AccountName")]
      string AzureDataLakeStoreAccountName { get; }

      [Option(Alias = "Azure.DataLake.SubscriptionId")]
      string AzureDataLakeSubscriptionId { get; }

      [Option(Alias = "Azure.KeyVault.Uri")]
      Uri KeyVaultUri { get; }

      [Option(Alias = "Azure.KeyVault.Creds")]
      NetworkCredential KeyVaultCreds { get; }

      [Option(Alias = "Azure.Databricks.BaseUri")]
      string DatabricksBaseUri { get; }

      [Option(Alias = "Azure.Databricks.Token")]
      string DatabricksToken { get; }

      #endregion

      #region [ Amazon Web Services ]

      [Option(Alias = "Aws.AccessKeyId")]
      string AwsAccessKeyId { get; }

      [Option(Alias = "Aws.SecretAccessKey")]
      string AwsSecretAccessKey { get; }

      [Option(Alias = "Aws.TestBucketName")]
      string AwsTestBucketName { get; }

      [Option(Alias = "Aws.TestBucketRegion", DefaultValue = "eu-west-1")]
      string AwsTestBucketRegion { get; }

      #endregion

      #region [ Google Cloud Platform ]

      [Option(Alias = "Gcp.Storage.BucketName")]
      string GcpStorageBucketName { get; }

      [Option(Alias = "Gcp.Storage.JsonKey")]
      string GcpStorageJsonCreds { get; }

      #endregion


      #region [ MSSQL ]

      [Option(Alias = "Mssql.ConnectionString")]
      string MssqlConnectionString { get; }

      #endregion

      #region [ General ]

      [Option(Alias = "Ftp.Hostname")]
      string FtpHostName { get; }

      [Option(Alias = "Ftp.Username")]
      string FtpUsername { get; }

      [Option(Alias = "Ftp.Password")]
      string FtpPassword { get; }

      #endregion
   }

   public static class Settings
   {
      private static ITestSettings _instance;

      public static ITestSettings Instance
      {
         get
         {
            if(_instance == null)
            {
               _instance = new ConfigurationBuilder<ITestSettings>()
                  .UseIniFile("c:\\tmp\\integration-tests.ini")
                  .UseEnvironmentVariables()
                  .Build();
            }

            return _instance;
         }
      }
   }
}
