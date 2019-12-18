using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Config.Net;
using Storage.Net.Messaging;
using Storage.Net.Tests;

namespace Storage.Net.ConsoleRunner
{
   class Program
   {
      static async Task Main(string[] args)
      {
         ITestSettings settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .Build();

         using(IMessenger messenger = StorageFactory.Messages.AzureEventHub(
            settings.EventHubConnectionString,
            settings.AzureStorageNativeConnectionString,
            null,
            "consoleeventhubs", "myprefix"))
         {
            await messenger.StartMessageProcessorAsync("integration", new Processor());

            Console.ReadKey();
         }
      }
   }

   class Processor : IMessageProcessor
   {
      public async Task ProcessMessagesAsync(IReadOnlyCollection<QueueMessage> messages)
      {
         foreach(QueueMessage qm in messages)
         {
            Console.WriteLine($"received {qm.Id}: {qm.StringContent}");
         }
      }
   }
}
