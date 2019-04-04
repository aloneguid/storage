using NetBox;
using Storage.Net.Blob;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Messaging;
using System.Threading;
using System.Collections.Generic;
using Config.Net;
using NetBox.Generator;
using NetBox.Extensions;
using Storage.Net.KeyValue;

namespace Storage.Net.Tests
{
   public class DocumentationScenarios
   {

      private readonly ITestSettings _settings;

      public DocumentationScenarios()
      {
         _settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .UseEnvironmentVariables()
            .Build();
      }

      //[Fact]
      public void Run()
      {
         //Blobs_save_file_to_azure_storage_and_read_it_later();
         //Blobs_save_file_to_a_specific_folder();
         Blobs_list_files_in_a_folder().Wait();
      }

      public async Task Blobs_list_files_in_a_folder()
      {

         IKeyValueStorage kv = StorageFactory.KeyValue.AzureTableStorage("my account", "my key");

         IBlobStorage s = StorageFactory.Blobs.FromConnectionString("azure.blobs://...parameters...");


         IMessagePublisher publisher = StorageFactory.Messages.InMemoryPublisher("name");

         IMessageReceiver receiver = StorageFactory.Messages.InMemoryReceiver("name");

         IBlobStorage storage = StorageFactory.Blobs.AmazonS3BlobStorage(
            _settings.AwsAccessKeyId,
            _settings.AwsSecretAccessKey,
            _settings.AwsTestBucketName);

         await storage.WriteAsync("folder1/file1", RandomGenerator.RandomString.ToMemoryStream(), false);
         await storage.WriteAsync("folder1/file2", RandomGenerator.RandomString.ToMemoryStream(), false);
         await storage.WriteAsync("folder2/file1", RandomGenerator.RandomString.ToMemoryStream(), false);

         BlobId[] folderBlobs = (await storage.ListAsync(new ListOptions { FolderPath = "folder1", Recurse = true })).ToArray();
      }

      public async Task BlobStorage_sample1()
      {
         IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(
            _settings.AzureStorageName,
            _settings.AzureStorageKey);

         //upload it
         string content = "test content";
         using (var s = new MemoryStream(Encoding.UTF8.GetBytes(content)))
         {
            await storage.WriteAsync("someid", s);
         }

         //read back
         using (var s = new MemoryStream())
         {
            using (Stream ss = await storage.OpenReadAsync("someid"))
            {
               await ss.CopyToAsync(s);

               //content is now "test content"
               content = Encoding.UTF8.GetString(s.ToArray());
            }
         }
      }

      public async Task BlobStorage_sample2()
      {
         IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(
            _settings.AzureStorageName,
            _settings.AzureStorageKey);

         //upload it
         await storage.WriteTextAsync("someid", "test content");

         //read back
         string content = await storage.ReadTextAsync("someid");
      }

      public async Task Blobs_save_file_to_a_specific_folder()
      {
         //create the storage over a specific local directory
         IBlobStorage storage = StorageFactory.Blobs.DirectoryFiles(new DirectoryInfo("c:\\tmp\\files"));

         //write to the root folder
         await storage.WriteTextAsync("text.txt", "test content");

         string subfolderBlobId = StoragePath.Combine("level 0", "level 1", "in the folder.log");
         await storage.WriteTextAsync(subfolderBlobId, "test content");
      }

      public async Task List_all_files_in_a_folder()
      {
         IBlobStorage storage = StorageFactory.Blobs.DirectoryFiles(new DirectoryInfo("c:\\tmp\\files"));

         await storage.ListAsync(new ListOptions { Recurse = true });
         await storage.ListAsync(new ListOptions { FolderPath = "/folder1", Recurse = false });
      }

      public async Task Send_and_receive_eventhubs()
      {
         IMessagePublisher publisher = StorageFactory.Messages.AzureEventHubPublisher("connection string");

         await publisher.PutMessagesAsync(new[]
         {
            new QueueMessage("hey mate!")
         });

         IMessageReceiver receiver = StorageFactory.Messages.AzureEventHubReceiver("connection string", "hub path");
         await receiver.StartMessagePumpAsync(OnNewMessage);
      }

      public Task OnNewMessage(IEnumerable<QueueMessage> message)
      {
         return Task.FromResult(true);
         //Console.WriteLine($"message received, id: {message.Id}, content: '{message.StringContent}'");
      }
   }
}
