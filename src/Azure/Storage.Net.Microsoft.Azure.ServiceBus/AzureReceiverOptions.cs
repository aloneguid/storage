using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace Storage.Net.Microsoft.Azure.ServiceBus
{
   /// <summary>
   /// Class that represents Azure Subscription Options
   /// </summary>
   public class AzureReceiverOptions
   {
      /// <summary>
      /// Function that handles the exception received.
      /// </summary>
      public Func<ExceptionReceivedEventArgs, Task> ExceptionReceivedHandler { get; set; }

      /// <summary>
      /// Number of maximum concurrent calls.
      /// </summary>
      public int MaxConcurrentCalls { get; set; }

      /// <summary>
      /// Subscription's maximum renew duration.
      /// </summary>
      public TimeSpan MaxAutoRenewDuration { get; set; }

      /// <summary>
      /// Initializes a new instance with 1 minute session renewal and 1 concurrent call.
      /// </summary>
      public AzureReceiverOptions()
      {
         MaxConcurrentCalls = 1;
         MaxAutoRenewDuration = TimeSpan.FromMinutes(1);
      }
   }
}
