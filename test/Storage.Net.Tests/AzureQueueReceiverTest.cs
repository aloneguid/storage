using Moq;
using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests
{
    public class AzureQueueReceiverTest
    {
        private readonly Mock<IMessageReceiver> mockMessage;

        public AzureQueueReceiverTest()
        {
            mockMessage = new Mock<IMessageReceiver>();
        }

        [Fact]
        public void StartMessagePumpAsync_ValidParams_Success()
        {
            mockMessage.Setup(s => s.StartMessagePumpAsync(It.IsAny<Func<IReadOnlyCollection<QueueMessage>, Task>>(), 1, It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            var result = mockMessage.Object.StartMessagePumpAsync(s => Task.CompletedTask, 1, CancellationToken.None);
            Assert.Equal(Task.CompletedTask, result);
        }
    }
}
