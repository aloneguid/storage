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

      [Option(Alias = "Azure.EventHub.Path")]
      string EventHubPath { get; }

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

      #endregion

      #region [ Amazon Web Services ]

      [Option(Alias = "Aws.AccessKeyId")]
      string AwsAccessKeyId { get; }

      [Option(Alias = "Aws.SecretAccessKey")]
      string AwsSecretAccessKey { get; }

      [Option(Alias = "Aws.TestBucketName")]
      string AwsTestBucketName { get; }

      #endregion

      #region [ MSSQL ]

      [Option(Alias = "Mssql.ConnectionString")]
      string MssqlConnectionString { get; }

      #endregion
   }
}
