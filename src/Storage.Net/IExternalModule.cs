using Storage.Net.ConnectionString;

namespace Storage.Net
{
   /// <summary>
   /// An entry point for implementing initialisation of an external module
   /// </summary>
   public interface IExternalModule
   {
      /// <summary>
      /// Gets connection factory
      /// </summary>
      IConnectionFactory ConnectionFactory { get; }
   }
}
