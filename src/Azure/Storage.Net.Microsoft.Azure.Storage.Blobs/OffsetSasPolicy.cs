using System;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   /// <summary>
   /// Base SAS policy with date and time offset
   /// </summary>
   public abstract class OffsetSasPolicy
   {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="startTime"></param>
      /// <param name="duration"></param>
      public OffsetSasPolicy(DateTimeOffset startTime, TimeSpan duration)
      {
         StartTime = startTime;
         Duration = duration;
      }

      /// <summary>
      /// Time when this policy starts
      /// </summary>
      public DateTimeOffset StartTime { get; set; }

      internal DateTimeOffset ExpiryTime => StartTime + Duration;

      /// <summary>
      /// Total duration of the SAS policy
      /// </summary>
      public TimeSpan Duration { get; set; }
   }
}
