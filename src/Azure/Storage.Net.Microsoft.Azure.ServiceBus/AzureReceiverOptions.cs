using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
      /// Function that handle the excepetion received.
      /// </summary>
      public Func<ExceptionReceivedEventArgs, Task> ExceptionReceivedHandler { get; set; }

      /// <summary>
      /// Number of maximum concurrent calls.
      /// </summary>
      public int MaxConcurrentCalls { get; set; }

      /// <summary>
      /// Subscription's maximum renew duration .
      /// </summary>
      public TimeSpan MaxAutoRenewDuration { get; set; }
      
      /// <summary>
      /// Initializes a new instance of <see cref="AzureReceiverOptions"/>.
      /// </summary>
      public AzureReceiverOptions()
      {
         MaxConcurrentCalls = 1;
         MaxAutoRenewDuration = TimeSpan.FromMinutes(1);
      }

   }
}
