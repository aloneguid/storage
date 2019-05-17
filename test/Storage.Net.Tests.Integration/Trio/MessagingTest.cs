using Xunit;
using Storage.Net.Messaging;
using System;
using System.Threading.Tasks;
using System.Linq;
using NetBox.Generator;
using System.Diagnostics;

namespace Storage.Net.Tests.Integration.Messaging
{

   public abstract class MessagingTest : IAsyncLifetime
   {
      private readonly MessagingFixture _fixture;

      protected MessagingTest(MessagingFixture fixture)
      {
         _fixture = fixture;
      }

      public async Task InitializeAsync()
      {
         await _fixture.StartPumpAsync();
      }

      public Task DisposeAsync() => Task.CompletedTask;

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

         string tag = await _fixture.PutMessageAsync(new QueueMessage(content));

         QueueMessage received = await _fixture.WaitMessageAsync(tag);

         Assert.True(received != null, $"no messages received with tag {tag}, {_fixture.GetMessageCount()} received in total");
         Assert.Equal(content, received.StringContent);
      }

      [Fact]
      public async Task SendMessage_WithProperties_Received()
      {
         string content = RandomGenerator.RandomString;

         var msg = new QueueMessage(content);
         msg.Properties["one"] = "v1";

         string tag = await _fixture.PutMessageAsync(msg);

         QueueMessage received = await _fixture.WaitMessageAsync(tag);

         Assert.True(received != null, "no message received with tag " + tag);
         Assert.Equal(content, received.StringContent);
         Assert.Equal("v1", received.Properties["one"]);
      }

      [Fact]
      public async Task CleanQueue_SendMessage_ReceiveAndConfirm()
      {
         string content = RandomGenerator.RandomString;
         var msg = new QueueMessage(content);
         string tag = await _fixture.PutMessageAsync(msg);

         QueueMessage rmsg = await _fixture.WaitMessageAsync(tag);
         Assert.NotNull(rmsg);
      }

      [Fact]
      public async Task MessagePump_AddFewMessages_CanReceiveOneAndPumpClearsThemAll()
      {
         QueueMessage[] messages = Enumerable.Range(0, 10)
            .Select(i => new QueueMessage(nameof(MessagePump_AddFewMessages_CanReceiveOneAndPumpClearsThemAll) + "#" + i))
            .ToArray();

         int prevCount = _fixture.GetMessageCount();
         await _fixture.Publisher.PutMessagesAsync(messages);
         await Task.Delay(TimeSpan.FromSeconds(10));
         int nowCount = _fixture.GetMessageCount();

         Assert.True(nowCount - prevCount >= 10, $"was: {prevCount}, now: {nowCount}, needs at least 10");
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