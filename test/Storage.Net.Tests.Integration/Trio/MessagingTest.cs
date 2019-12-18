using Xunit;
using Storage.Net.Messaging;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Storage.Net.Microsoft.Azure.ServiceBus;
using System.Linq;
using System.Threading;

namespace Storage.Net.Tests.Integration.Messaging
{
   [Trait("Category", "Messenger")]
   public abstract class MessagingTest : IAsyncLifetime
   {
      private readonly MessagingFixture _fixture;
      private readonly string _channelPrefix;
      private readonly string _receiveChannelSuffix;
      private readonly IMessenger _msg;
      private readonly string _qn;

      protected MessagingTest(MessagingFixture fixture, string channelPrefix = null, string channelFixedName = null, string receiveChannelSuffix = null)
      {
         _fixture = fixture;
         _channelPrefix = channelPrefix;
         _qn = channelFixedName ?? NewChannelName();
         _receiveChannelSuffix = receiveChannelSuffix;
         _msg = fixture.Messenger;
      }

      public async Task InitializeAsync()
      {
         try
         {
            await _msg.CreateChannelAsync(_qn);
         }
         catch(NotSupportedException)
         {

         }
      }

      private string NewChannelName()
      {
         return $"{_channelPrefix}{Guid.NewGuid().ToString()}";
      }

      public async Task DisposeAsync()
      {
         //clear up all the channels

         try
         {
            await _msg.DeleteChannelAsync(_qn);
         }
         catch { }
      }

      [Fact]
      public async Task SendMessage_OneMessage_DoesntCrash()
      {
         var qm = QueueMessage.FromText("test");

         await _msg.SendAsync(_qn, qm);
      }

      [Fact]
      public async Task SendMessage_NullChannel_ArgumentException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.SendAsync(null, QueueMessage.FromText("test")));
      }

      [Fact]
      public async Task SendMessage_NullMessages_ArgumentException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.SendAsync(_qn, null));
      }

      [Fact]
      public async Task Channels_list_doesnt_crash()
      {
         await _msg.ListChannelsAsync();
      }

      [Fact]
      public async Task Channels_Create_list_contains_created_channel()
      {
         string channelName = NewChannelName();

         try
         {
            await _msg.CreateChannelAsync(channelName);
         }
         catch(NotSupportedException)
         {
            return;
         }

         //some providers don't list channels immediately as they are eventually consistent

         const int maxRetries = 10;

         for(int i = 0; i < maxRetries; i++)
         {
            IReadOnlyCollection<string> channels = await _msg.ListChannelsAsync();

            if(channels.Contains(channelName))
               return;

            await Task.Delay(TimeSpan.FromSeconds(5));
         }

         Assert.True(false, $"channel not found after {maxRetries} retries.");
      }

      [Fact]
      public async Task Channels_delete_goesaway()
      {
         string channelName = NewChannelName();

         try
         {
            await _msg.CreateChannelAsync(channelName);
         }
         catch(NotSupportedException)
         {
            return;
         }

         await _msg.DeleteChannelAsync(channelName);

         IReadOnlyCollection<string> channels = await _msg.ListChannelsAsync();

         Assert.DoesNotContain(channelName, channels);

      }

      [Fact]
      public async Task Channels_delete_null_list_argument_exception()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.DeleteChannelsAsync(null));
      }

      [Fact]
      public async Task MessageCount_Send_One_Count_Changes()
      {
         long count1;

         try
         {
            count1 = await _msg.GetMessageCountAsync(_qn + _receiveChannelSuffix);
         }
         catch(NotSupportedException)
         {
            return;
         }

         await _msg.SendAsync(_qn, QueueMessage.FromText("bla bla"));

         long count2 = await _msg.GetMessageCountAsync(_qn + _receiveChannelSuffix);

         Assert.NotEqual(count1, count2);
      }

      [Fact]
      public async Task MessageCount_Null_ThrowsArgumentNull()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.GetMessageCountAsync(null));
      }

      [Fact]
      public async Task MessageCount_NonExistentQueue_Return0()
      {
         try
         {
            Assert.Equal(0, await _msg.GetMessageCountAsync(NewChannelName() + _receiveChannelSuffix));
         }
         catch(NotSupportedException)
         {

         }
      }

      [Fact]
      public async Task Receive_SendOne_Received()
      {
         string tag = await SendAsync();

         try
         {
            IReadOnlyCollection<QueueMessage> messages = await _msg.ReceiveAsync(_qn);

            Assert.Contains(messages, m => m.Properties.TryGetValue("tag", out string itag) && itag == tag);
         }
         catch(NotSupportedException)
         {

         }
      }

      [Fact]
      public async Task Peek_NullChannel_ThrowsArgumentNull()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.PeekAsync(null));
      }

      [Fact]
      public async Task Peek_SendMessage_HasAtLeaseOne()
      {
         try
         {
            await SendAsync();

            IReadOnlyCollection<QueueMessage> messages = await _msg.PeekAsync(_qn);

            Assert.NotEmpty(messages);
         }
         catch(NotSupportedException)
         {

         }
      }

      [Fact]
      public async Task Receive_NullChannel_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _msg.ReceiveAsync(null));
      }

      private async Task<string> SendAsync()
      {
         string tag = Guid.NewGuid().ToString();

         var msg = QueueMessage.FromText("hm");
         msg.Properties["tag"] = tag;

         await _msg.SendAsync(_qn, msg);

         return tag;
      }
   }
}