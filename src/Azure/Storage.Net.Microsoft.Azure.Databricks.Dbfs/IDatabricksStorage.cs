using System.Threading.Tasks;
using Microsoft.Azure.Databricks.Client;
using Storage.Net.Blobs;

namespace Storage.Net.Databricks
{
   /// <summary>
   /// Databricks specific functionality
   /// </summary>
   public interface IDatabricksStorage : IBlobStorage
   {
      /// <summary>
      /// Gets native reference. Do not use as there is no guarantee I won't switch the library in future.
      /// </summary>
      /// <returns></returns>
      DatabricksClient GetMicrosoftAzureDatabrickClientLibraryClient();

      /// <summary>
      /// Crates a new secret scope. If scope already exists the call succeeds silently.
      /// Note also that scopes are created automatically when you try to write a secret (/secrets/scope_name/key).
      /// </summary>
      /// <param name="name"></param>
      /// <param name="initialManagePrincipal"></param>
      /// <returns></returns>
      Task CreateSecretsScope(string name, string initialManagePrincipal = null);
   }
}
