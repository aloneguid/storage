using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Messaging;

namespace Storage.Net.Messaging.Files
{
   /// <summary>
   /// Messages themselves can be human readable. THe speed is not an issue because the main bottleneck is disk anyway.
   /// </summary>
   class DiskMessagePublisherReceiver : PollingMessageReceiver, IMessagePublisher
   {
      private const string FileExtension = ".snm";

      private readonly string _root;

      public DiskMessagePublisherReceiver(string directoryPath)
      {
         _root = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));

         if(!Directory.Exists(_root))
            Directory.CreateDirectory(_root);
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
         //get all files (not efficient, but we hope there won't be many)
         FileInfo[] files = new DirectoryInfo(_root).GetFiles("*" + FileExtension, SearchOption.TopDirectoryOnly);

         //sort files so that oldest appear first, take max and return
         return Task.FromResult<IReadOnlyCollection<QueueMessage>>(files
            .OrderBy(f => f.Name)
            .Take(maxBatchSize)
            .Select(f => File.ReadAllBytes(f.FullName))
            .Select(QueueMessage.FromByteArray)
            .ToList());
      }

      #endregion

      private static string GenerateDiskId()
      {
         DateTime now = DateTime.UtcNow;

         //generate sortable file name, so that we can get the oldest item or newest item easily
         return now.ToString("yyyy-MM-dd-hh-mm-ss-ffff") + FileExtension;
      }
   }
}