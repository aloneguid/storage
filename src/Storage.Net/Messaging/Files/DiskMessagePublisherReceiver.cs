using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Messaging;

namespace Storage.Net.Queue.Files
{
   /// <summary>
   /// Messages themselves can be human readable. THe speed is not an issue because the main bottleneck is disk anyway.
   /// </summary>
   class DiskMessagePublisherReceiver : PollingMessageReceiver, IMessagePublisher
   {
      private readonly string _root;

      public DiskMessagePublisherReceiver(string directoryPath)
      {
         _root = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));
      }

      #region [ Publisher ]

      public Task PutMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         foreach (QueueMessage msg in messages)
         {
            string filePath = Path.Combine(_root, GenerateDiskId());

            File.WriteAllBytes(filePath, msg.ToByteArray());
         }

         return Task.FromResult(true);
      }

      #endregion

      #region [ Receiver ]

      protected override Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken)
      {
         throw new NotImplementedException();
      }

      #endregion

      private static string GenerateDiskId()
      {
         DateTime now = DateTime.UtcNow;

         return now.ToString("yyyy-MM-dd-hh-mm-ss-ffff") + ".txt";
      }
   }
}