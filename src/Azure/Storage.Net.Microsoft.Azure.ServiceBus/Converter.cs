using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using QueueMessage = Storage.Net.Messaging.QueueMessage;

namespace Storage.Net.Microsoft.Azure.ServiceBus
{
   static class Converter
   {
      public static Message ToMessage(QueueMessage message)
      {
         if(message == null)
            throw new ArgumentNullException(nameof(message));

         var result = new Message(message.Content);
         if(message.Properties != null && message.Properties.Count > 0)
         {
            foreach(KeyValuePair<string, string> prop in message.Properties)
            {
               result.UserProperties.Add(prop.Key, prop.Value);
            }
         }
         return result;
      }

      public static QueueMessage ToQueueMessage(Message message)
      {
         string id = message.MessageId ?? message.SystemProperties.SequenceNumber.ToString();

         var result = new QueueMessage(id, message.Body);
         result.DequeueCount = message.SystemProperties.DeliveryCount;
         if(message.UserProperties != null && message.UserProperties.Count > 0)
         {
            foreach(KeyValuePair<string, object> pair in message.UserProperties)
            {
               result.Properties[pair.Key] = pair.Value == null ? null : pair.Value.ToString();
            }
         }

         return result;
      }
   }
}
