using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Storage.Net.Messaging;

namespace Storage.Net.Amazon.Aws.Messaging
{
   class AwsS3MessageReceiver : PollingMessageReceiver
   {
      private readonly AmazonSQSClient _client;
      private readonly string _queueUrl;

      public AwsS3MessageReceiver(string accessKeyId, string secretAccessKey, string serviceUrl, string queueName, RegionEndpoint regionEndpoint)
      {
         var config = new AmazonSQSConfig
         {
            ServiceURL = serviceUrl,
            RegionEndpoint = regionEndpoint ?? RegionEndpoint.USEast1
         };

         _client = new AmazonSQSClient(new BasicAWSCredentials(accessKeyId, secretAccessKey), config);
         _queueUrl = new Uri(new Uri(serviceUrl), queueName).ToString();   //convert safely to string
      }

      public override async Task<int> GetMessageCountAsync()
      {
         GetQueueAttributesResponse attrs = await _client.GetQueueAttributesAsync(_queueUrl, new List<string> { "All" }).ConfigureAwait(false);

         return attrs.ApproximateNumberOfMessages;
      }

      public override async Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         var request = new DeleteMessageBatchRequest(_queueUrl,
            messages.Select(m => new DeleteMessageBatchRequestEntry(m.Id, m.Properties[Converter.ReceiptHandlePropertyName])).ToList());

         await _client.DeleteMessageBatchAsync(request, cancellationToken).ConfigureAwait(false);
      }

      protected override async Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken)
      {
         var request = new ReceiveMessageRequest(_queueUrl)
         {
            MessageAttributeNames = new List<string> { ".*" },
            MaxNumberOfMessages = Math.Min(10, maxBatchSize)
         };

         ReceiveMessageResponse messages = await _client.ReceiveMessageAsync(request, cancellationToken).ConfigureAwait(false);

         return messages.Messages.Select(Converter.ToQueueMessage).ToList();
      }

      public override async Task<IReadOnlyCollection<QueueMessage>> PeekMessagesAsync(int maxMessages, CancellationToken cancellationToken = default)
      {
         var request = new ReceiveMessageRequest(_queueUrl)
         {
            MessageAttributeNames = new List<string> { ".*" },
            MaxNumberOfMessages = maxMessages,

            //AWS doesn't have peek method, however setting visibility timeout to minimum (1 second!) we can simulate that
            VisibilityTimeout = 1
         };

         ReceiveMessageResponse messages = await _client.ReceiveMessageAsync(request, cancellationToken).ConfigureAwait(false);
         return messages.Messages.Select(Converter.ToQueueMessage).ToList();
      }

      public override Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }
   }
}
