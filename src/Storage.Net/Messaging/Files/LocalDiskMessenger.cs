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
   class LocalDiskMessenger : IMessenger
   {
      private const string FileExtension = ".snm";

      private readonly string _root;

      public LocalDiskMessenger(string directoryPath)
      {
         _root = directoryPath ?? throw new ArgumentNullException(nameof(directoryPath));
      }

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

      private IReadOnlyCollection<FileInfo> GetMessageFiles(string channelName)
      {
         string directoryPath = Path.Combine(_root, channelName);

         if(!Directory.Exists(directoryPath))
            return new List<FileInfo>();

         return new DirectoryInfo(directoryPath).GetFiles("*" + FileExtension, SearchOption.TopDirectoryOnly);
      }

      private string GetMessagePath(string channelName)
      {
         string dir = Path.Combine(_root, channelName);
         if(!Directory.Exists(dir))
            Directory.CreateDirectory(dir);

         return Path.Combine(dir, GenerateDiskId());
      }

      #region [ IMessenger ]

      public Task CreateChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellation = default)
      {
         foreach(string channelName in channelNames)
         {
            string path = Path.Combine(_root, channelName);
            if(!Directory.Exists(path))
               Directory.CreateDirectory(path);
         }

         return Task.CompletedTask;
      }


      public Task<IReadOnlyCollection<string>> ListChannelsAsync(CancellationToken cancellationToken = default)
      {
         return Task.FromResult<IReadOnlyCollection<string>>(new DirectoryInfo(_root).GetDirectories().Select(d => d.Name).ToList());
      }

      public Task DeleteChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default)
      {
         if(channelNames is null)
            throw new ArgumentNullException(nameof(channelNames));

         foreach(string channelName in channelNames)
         {
            string dir = Path.Combine(_root, channelName);
            if(Directory.Exists(dir))
               Directory.Delete(dir, true);
         }

         return Task.CompletedTask;
      }

      public Task<long> GetMessageCountAsync(string channelName, CancellationToken cancellationToken = default)
      {
         return Task.FromResult<long>(GetMessageFiles(channelName).Count);
      }

      public Task SendAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if(channelName is null)
            throw new ArgumentNullException(nameof(channelName));

         if(messages is null)
            throw new ArgumentNullException(nameof(messages));

         foreach(QueueMessage msg in messages)
         {
            if(msg == null)
               throw new ArgumentNullException(nameof(msg));

            string filePath = GetMessagePath(channelName);

            File.WriteAllBytes(filePath, msg.ToByteArray());
         }

         return Task.FromResult(true);
      }

      public Task<IReadOnlyCollection<QueueMessage>> ReceiveAsync(
         string channelName,
         int count = 100,
         TimeSpan? visibility = null,
         CancellationToken cancellationToken = default)
      {
         return Task.FromResult(GetMessages(channelName, count));
      }

      public Task<IReadOnlyCollection<QueueMessage>> PeekAsync(string channelName, int count = 100, CancellationToken cancellationToken = default)
      {
         return Task.FromResult(GetMessages(channelName, count));
      }

      public void Dispose()
      {

      }

      public Task DeleteAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      #endregion

      private IReadOnlyCollection<QueueMessage> GetMessages(
         string channelName,
         int count)
      {
         //get all files (not efficient, but we hope there won't be many)
         IReadOnlyCollection<FileInfo> files = GetMessageFiles(channelName);

         //sort files so that oldest appear first, take max and return
         return files
            .OrderBy(f => f.Name)
            .Take(count)
            .Select(ToQueueMessage)
            .ToList();
      }

      public Task StartMessageProcessorAsync(string channelName, IMessageProcessor messageProcessor) => throw new NotImplementedException();
   }
}