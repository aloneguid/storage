using System;
using System.Collections.Generic;
using System.Text;

namespace Storage.Net.Messaging.Polling
{
   class ExponentialBackoffPollingPolicy : IPollingPolicy
   {
      private readonly TimeSpan _initialDelay;
      private readonly TimeSpan _maxDelay;
      private TimeSpan _currentDelay;

      public ExponentialBackoffPollingPolicy(TimeSpan initialDelay, TimeSpan maxDelay)
      {
         _initialDelay = initialDelay;
         _maxDelay = maxDelay;
         _currentDelay = _initialDelay;
      }

      public TimeSpan GetNextDelay()
      {
         TimeSpan result = _currentDelay;

         _currentDelay = TimeSpan.FromMilliseconds(_currentDelay.TotalMilliseconds * 2);
         if(_currentDelay > _maxDelay)
         {
            _currentDelay = _initialDelay;
         }

         return result;
      }

      public void Reset()
      {
         _currentDelay = _initialDelay;
      }
   }
}
