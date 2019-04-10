using Xunit;
using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Config.Net;
using NetBox.Generator;
using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

namespace Storage.Net.Tests.Integration.Messaging
{
   public abstract class MessagingFixture : IDisposable
   {
      private static readonly ITestSettings _settings;
      public readonly IMessagePublisher Publisher;
      public readonly IMessageReceiver Receiver;
      private readonly ConcurrentDictionary<string, QueueMessage> _receivedMessages = new ConcurrentDictionary<string, QueueMessage>();
      private QueueMessage _lastReceivedMessage;
      private int _receivedCount;

      private bool _pumpStarted = false;
      private readonly CancellationTokenSource _cts = new CancellationTokenSource();
      protected readonly string _testDir;

      static MessagingFixture()
      {
         _settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .UseEnvironmentVariables()
            .Build();
      }

      public MessagingFixture()
      {
         string buildDir = new FileInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath).Directory.FullName;
         _testDir = Path.Combine(buildDir, "TEST-" + Guid.NewGuid());
         Directory.CreateDirectory(_testDir);

         Publisher = CreatePublisher(_settings);
         Receiver = CreateReceiver(_settings);
      }

      protected abstract IMessagePublisher CreatePublisher(ITestSettings settings);

      protected abstract IMessageReceiver CreateReceiver(ITestSettings settings);

      public async Task StartPumpAsync()
      {
         if(_pumpStarted || Receiver == null)
            return;

         _pumpStarted = true;

         //start the pump
         await Receiver.StartMessagePumpAsync(ReceiverPumpAsync, cancellationToken: _cts.Token, maxBatchSize: 500);
      }

      private async Task ReceiverPumpAsync(IReadOnlyCollection<QueueMessage> messages)
      {
         foreach(QueueMessage qm in messages)
         {
            string tag = qm.Properties.ContainsKey("tag")
               ? qm.Properties["tag"]
               : null;

            Debug.WriteLine("received tag: " + tag);
            Console.WriteLine("received tag: " + tag);

            if(!_receivedMessages.TryAdd(tag ?? Guid.NewGuid().ToString(), qm))
            {
               throw new InvalidOperationException("could not add tag");
            }
            _lastReceivedMessage = qm;
            Interlocked.Increment(ref _receivedCount);
         }

         try
         {
            await Receiver.ConfirmMessagesAsync(messages);
         }
         catch(NotSupportedException)
         {
            //some provides may not support this
         }
      }

      public QueueMessage GetTaggedMessage(string tag)
      {
         if(tag == null)
            return _lastReceivedMessage;

         if(!_receivedMessages.TryGetValue(tag, out QueueMessage result))
            return null;

         return result;
      }

      public int GetMessageCount() => _receivedCount;

      public void Dispose()
      {
         _cts.Cancel();

         if(Publisher != null)
            Publisher.Dispose();

         if(Receiver != null)
            Receiver.Dispose();
      }
   }

   public abstract class MessagingTest : IAsyncLifetime
   {
      private static readonly TimeSpan MaxWaitTime = TimeSpan.FromMinutes(1);


      private readonly MessagingFixture _fixture;

      public MessagingTest(MessagingFixture fixture)
      {
         _fixture = fixture;
      }

      public async Task InitializeAsync()
      {
         await _fixture.StartPumpAsync();
      }

      public Task DisposeAsync() => Task.CompletedTask;

      private async Task<string> PutMessageAsync(QueueMessage message)
      {
         string tag = Guid.NewGuid().ToString();
         message.Properties["tag"] = tag;

         await _fixture.Publisher.PutMessagesAsync(new[] { message });

         return tag;
      }

      private async Task<QueueMessage> WaitMessage(string tag, TimeSpan? maxWaitTime = null, int minCount = 1)
      {
         DateTime start = DateTime.UtcNow;

         while((DateTime.UtcNow - start) < (maxWaitTime ?? MaxWaitTime))
         {
            QueueMessage candidate = _fixture.GetTaggedMessage(tag);

            if(candidate != null && _fixture.GetMessageCount() >= minCount)
            {
               return candidate;
            }

            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
         }

         return null;
      }

      [Fact]
      public async Task SendMessage_OneMessage_DoesntCrash()
      {
         var qm = QueueMessage.FromText("test");
         await _fixture.Publisher.PutMessagesAsync(new[] { qm });
      }

      [Fact]
      public async Task SendMessage_Null_ThrowsArgumentNull()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _fixture.Publisher.PutMessageAsync(null));
      }

      [Fact]
      public async Task SendMessages_LargeAmount_Succeeds()
      {
         await _fixture.Publisher.PutMessagesAsync(Enumerable.Range(0, 100).Select(i => QueueMessage.FromText("message #" + i)).ToList());
      }

      [Fact]
      public async Task SendMessages_Null_DoesntFail()
      {
         await _fixture.Publisher.PutMessagesAsync(null);
      }

      [Fact]
      public async Task SendMessages_SomeNull_ThrowsArgumentNull()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _fixture.Publisher.PutMessagesAsync(new[] { QueueMessage.FromText("test"), null }));
      }

      [Fact]
      public async Task SendMessage_ExtraProperties_DoesntCrash()
      {
         var msg = new QueueMessage("prop content at " + DateTime.UtcNow);
         msg.Properties["one"] = "one value";
         msg.Properties["two"] = "two value";
         await _fixture.Publisher.PutMessagesAsync(new[] { msg });
      }

      [Fact]
      public async Task SendMessage_SimpleOne_Received()
      {
         string content = RandomGenerator.RandomString;

         string tag = await PutMessageAsync(new QueueMessage(content));

         QueueMessage received = await WaitMessage(tag);

         Assert.True(received != null, $"no messages received with tag {tag}, {_fixture.GetMessageCount()} received in total");
         Assert.Equal(content, received.StringContent);
      }

      [Fact]
      public async Task SendMessage_WithProperties_Received()
      {
         string content = RandomGenerator.RandomString;

         var msg = new QueueMessage(content);
         msg.Properties["one"] = "v1";

         string tag = await PutMessageAsync(msg);

         QueueMessage received = await WaitMessage(tag);

         Assert.True(received != null, "no message received with tag " + tag);
         Assert.Equal(content, received.StringContent);
         Assert.Equal("v1", received.Properties["one"]);
      }

      [Fact]
      public async Task CleanQueue_SendMessage_ReceiveAndConfirm()
      {
         string content = RandomGenerator.RandomString;
         var msg = new QueueMessage(content);
         string tag = await PutMessageAsync(msg);

         QueueMessage rmsg = await WaitMessage(tag);
         Assert.NotNull(rmsg);
      }

      [Fact]
      public async Task MessagePump_AddFewMessages_CanReceiveOneAndPumpClearsThemAll()
      {
         QueueMessage[] messages = Enumerable.Range(0, 10)
            .Select(i => new QueueMessage(nameof(MessagePump_AddFewMessages_CanReceiveOneAndPumpClearsThemAll) + "#" + i))
            .ToArray();

         await _fixture.Publisher.PutMessagesAsync(messages);

         await WaitMessage(null, null, 10);

         Assert.True(_fixture.GetMessageCount() >= 10, _fixture.GetMessageCount().ToString());
      }

      [Fact]
      public async Task MessageCount_IsGreaterThanZero()
      {
         //put quite a few messages

         await _fixture.Publisher.PutMessagesAsync(Enumerable.Range(0, 100).Select(i => QueueMessage.FromText("message #" + i)).ToList());

         try
         {
            int count = await _fixture.Receiver.GetMessageCountAsync();

            Assert.True(count > 0);
         }
         catch(NotSupportedException)
         {
            //not all providers support this
         }
      }
   }
}