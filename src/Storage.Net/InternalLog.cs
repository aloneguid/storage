using System.Diagnostics;

namespace Storage.Net
{
   /// <summary>
   /// Internal logging method
   /// </summary>
   public static class InternalLog
   {
      public static bool IsEnabled { get; set; }

      public static void Log(string s)
      {
         Debug.WriteLine(s);
      }
   }
}
