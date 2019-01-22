using System;
using System.Collections.Generic;
using System.Text;

namespace Storage.Net.ConnectionString
{
   /// <summary>
   /// Holds a parsed connection string to the storage
   /// </summary>
   public class StorageConnectionString
   {
      private const string PrefixSeparator = "://";
      private static readonly char[] PartsSeparators = new[] { ';' };
      private static readonly char[] PartSeparator = new[] { '=' };

      private readonly Dictionary<string, string> _parts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

      /// <summary>
      /// Creates a new instance of <see cref="StorageConnectionString"/>
      /// </summary>
      /// <param name="connectionString"></param>
      public StorageConnectionString(string connectionString)
      {
         ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));

         Parse(connectionString);
      }

      /// <summary>
      /// Original connection string
      /// </summary>
      public string ConnectionString { get; private set; }

      /// <summary>
      /// Prefix of this connection string, excluding prefix separator, i.e. for 'disk://something' the prefix is 'disk'
      /// </summary>
      public string Prefix { get; private set; }

      /// <summary>
      /// Gets the value of the parameter as when it's required. When parameter is not present, throws standard <see cref="ArgumentException"/>
      /// </summary>
      /// <param name="parameterName">Parameter name</param>
      /// <param name="requireNonEmptyValue">When true, checks that parameter value is not null or empty and throws <see cref="ArgumentException"/></param>
      /// <param name="value">Result value</param>
      public void GetRequired(string parameterName, bool requireNonEmptyValue, out string value)
      {
         if (parameterName == null)
         {
            throw new ArgumentNullException(nameof(parameterName));
         }

         if (!_parts.TryGetValue(parameterName, out value))
         {
            throw new ArgumentException($"connection string requires '{parameterName}' parameter.");
         }

         if(requireNonEmptyValue && string.IsNullOrEmpty(value))
         {
            throw new ArgumentException($"parameter '{parameterName}' is present but value is not set.");
         }
      }

      /// <summary>
      /// Get connection string parameter by name
      /// </summary>
      /// <param name="parameterName"></param>
      /// <returns>Parameter value. If parameter is not set returns an empty string</returns>
      public string Get(string parameterName)
      {
         if (parameterName == null) return string.Empty;
         if (!_parts.TryGetValue(parameterName, out string value)) return string.Empty;
         return value;
      }

      private void Parse(string connectionString)
      {
         int idx = connectionString.IndexOf(PrefixSeparator);

         if(idx == -1)
         {
            throw new ArgumentException($"prefix separator ({PrefixSeparator}) not present", nameof(connectionString));
         }

         Prefix = connectionString.Substring(0, idx);
         connectionString = connectionString.Substring(idx + PrefixSeparator.Length);

         // prefix extracted, now get the parts of the string

         string[] parts = connectionString.Split(PartsSeparators, StringSplitOptions.RemoveEmptyEntries);
         foreach(string part in parts)
         {
            string[] kv = part.Split(PartSeparator, 2);

            string key = kv[0];
            string value = kv.Length == 1 ? string.Empty : kv[1];
            _parts[key] = value;
         }
      }
   }
}
