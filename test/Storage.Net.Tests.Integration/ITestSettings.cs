using System;
using System.Net;
using Config.Net;

namespace Storage.Net.Tests
{
   public interface ITestSettings
   {
      [Option(DefaultValue = "aloneguid")]
      string DevOpsOrgName { get; }

      [Option(DefaultValue = "AllPublic")]
      string DevOpsProject { get; }

      [Option(DefaultValue = "8")]
      string DevOpsVariableSetId { get; }

      string DevOpsPat { get; }

      string ClientId { get; }

      string ClientSecret { get; }

      string TenantId { get; }

      #region [ Azure ]

      string AzureStorageName { get; }

      string AzureStorageKey { get; }

      string AzureGen2StorageName { get; }

      string AzureGen2StorageKey { get; }

      string OperatorObjectId { get; }

      string AzureServiceBusConnectionString { get; }

      string AzureEventHubConnectionString { get; }

      string AzureStorageNativeConnectionString { get; }

      string AzureGen1StorageName { get; }

      Uri AzureKeyVaultUri { get; }

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

      string DatabricksBaseUri { get; set; }

      string DatabricksToken { get; set; }
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

               _instance = new ConfigurationBuilder<ITestSettings>()
                  .UseIniFile("c:\\tmp\\integration-tests.ini")
                  .UseAzureDevOpsVariableSet(_instance.DevOpsOrgName, _instance.DevOpsProject, _instance.DevOpsPat, _instance.DevOpsVariableSetId)
                  .UseEnvironmentVariables()
                  .Build();

            }

            return _instance;
         }
      }
   }
}
