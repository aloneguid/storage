using System;

namespace Storage.Net.Messaging.Polling
{
   interface IPollingPolicy
   {
      void Reset();

      TimeSpan GetNextDelay();
   }
}
