using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Message processing interface used to register a callback that receives a message
   /// </summary>
   public interface IMessageProcessor
   {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="messages"></param>
      /// <returns></returns>
      Task ProcessMessagesAsync(IReadOnlyCollection<QueueMessage> messages);
   }
}
