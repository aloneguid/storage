using System;
using System.Collections.Generic;
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
         GetQueueAttributesResponse attrs = await _client.GetQueueAttributesAsync(_queueUrl, new List<string> { "All" });

         return attrs.ApproximateNumberOfMessages;
      }

      protected override async Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken)
      {
         throw new NotImplementedException();
      }
   }
}
