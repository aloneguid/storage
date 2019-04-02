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
   class AwsS3MessagePublisher : IMessagePublisher
   {
      private readonly AmazonSQSClient _client;
      private readonly string _queueName;
      private readonly string _queueUrl;

      /// <summary>
      /// 
      /// </summary>
      /// <param name="accessKeyId"></param>
      /// <param name="secretAccessKey"></param>
      /// <param name="serviceUrl">Serivce URL, for instance http://sqs.us-west-2.amazonaws.com"</param>
      /// <param name="queueName"></param>
      /// <param name="regionEndpoint">Optional regional endpoint</param>
      public AwsS3MessagePublisher(string accessKeyId, string secretAccessKey, string serviceUrl, string queueName, RegionEndpoint regionEndpoint)
      {
         var config = new AmazonSQSConfig
         {
            ServiceURL = serviceUrl,
            RegionEndpoint = regionEndpoint ?? RegionEndpoint.USEast1
         };

         _client = new AmazonSQSClient(new BasicAWSCredentials(accessKeyId, secretAccessKey), config);
         _queueName = queueName;
         _queueUrl = new Uri(new Uri(serviceUrl), queueName).ToString();   //convert safely to string
      }

      public async Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if(messages == null)
            return;

         var request = new SendMessageBatchRequest(
            _queueUrl,
            messages.Select(Converter.ToSQSMessage).ToList());

         SendMessageBatchResponse r = await _client.SendMessageBatchAsync(request, cancellationToken);
      }

      public void Dispose()
      {

      }


   }
}
