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

      public Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if(messages == null)
            return Task.FromResult(true);

         foreach (QueueMessage msg in messages)
         {
            if(msg == null)
               throw new ArgumentNullException(nameof(msg));

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
         IReadOnlyCollection<FileInfo> files = GetMessageFiles();

         //sort files so that oldest appear first, take max and return
         return Task.FromResult<IReadOnlyCollection<QueueMessage>>(files
            .OrderBy(f => f.Name)
            .Take(maxBatchSize)
            .Select(ToQueueMessage)
            .ToList());
      }

      #endregion

      private static string GenerateDiskId()
      {
         DateTime now = DateTime.UtcNow;

         //generate sortable file name, so that we can get the oldest item or newest item easily
         return now.ToString("yyyy-MM-dd-hh-mm-ss-ffff") + FileExtension;
      }

      private static QueueMessage ToQueueMessage(FileInfo fi)
      {
         byte[] content = File.ReadAllBytes(fi.FullName);

         var result = QueueMessage.FromByteArray(content);
         result.Id = Path.GetFileNameWithoutExtension(fi.Name);
         return result;
      }

      private IReadOnlyCollection<FileInfo> GetMessageFiles()
      {
         return new DirectoryInfo(_root).GetFiles("*" + FileExtension, SearchOption.TopDirectoryOnly);
      }

      public override Task<int> GetMessageCountAsync()
      {
         IReadOnlyCollection<FileInfo> files = GetMessageFiles();

         return Task.FromResult(files.Count);
      }

      public override Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         foreach(QueueMessage qm in messages)
         {
            string fullPath = Path.Combine(_root, qm.Id + FileExtension);
            if(File.Exists(fullPath))
               File.Delete(fullPath);
         }

         return Task.FromResult(true);
      }

      public override Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default) => throw new NotSupportedException();
   }
}