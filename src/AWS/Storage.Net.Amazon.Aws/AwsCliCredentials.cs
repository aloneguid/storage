#if !NET16
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Amazon.Runtime;

namespace Storage.Net.Amazon.Aws
{
   /// <summary>
   /// Provides helper utilities to read profiles from ~/.aws/credentials file
   /// </summary>
   public static class AwsCliCredentials
   {
      private const string KeyIdKeyName = "aws_access_key_id";
      private const string AccessKeyKeyName = "aws_secret_access_key";
      private const string SessionTokenKeyName = "aws_session_token";

      /// <summary>
      /// Reads the list of profile names.
      /// </summary>
      /// <returns></returns>
      public static IReadOnlyCollection<string> EnumerateProfiles()
      {
         return ReadProfiles(GetCredentialsPath()).Keys.ToList();
      }

      /// <summary>
      /// Creates a raw form of credentials as key-value pairs.
      /// </summary>
      /// <param name="profileName"></param>
      /// <returns></returns>
      public static IDictionary<string, string> GetRawCredentials(string profileName)
      {
         Dictionary<string, Dictionary<string, string>> profiles = ReadProfiles(GetCredentialsPath());

         if(!profiles.TryGetValue(profileName, out Dictionary<string, string> profile))
         {
            throw new ArgumentException($"profile '{profileName}' does not exist", nameof(profileName));
         }

         return profile;
      }

      /// <summary>
      /// Creates a native <see cref="AWSCredentials"/> base on profile data
      /// </summary>
      /// <param name="profileName"></param>
      /// <returns></returns>
      public static AWSCredentials GetCredentials(string profileName)
      {
         Dictionary<string, Dictionary<string, string>> profiles = ReadProfiles(GetCredentialsPath());

         if(!profiles.TryGetValue(profileName, out Dictionary<string, string> profile))
         {
            throw new ArgumentException($"profile '{profileName}' does not exist", nameof(profileName));
         }

         if(!profile.TryGetValue(KeyIdKeyName, out string keyId) || !profile.TryGetValue(AccessKeyKeyName, out string accessKey))
         {
            throw new ArgumentException($"both '{KeyIdKeyName}' and '{AccessKeyKeyName}' must be present in the profile");
         }

         if(profile.TryGetValue(SessionTokenKeyName, out string sessionToken) && !string.IsNullOrEmpty(sessionToken))
         {
            return new SessionAWSCredentials(keyId, accessKey, sessionToken);
         }

         return new BasicAWSCredentials(keyId, accessKey);
      }

      private static Dictionary<string, Dictionary<string, string>> ReadProfiles(string path)
      {
         var profiles = new Dictionary<string, Dictionary<string, string>>();

         var profile = new Dictionary<string, string>();
         string profileName = null;

         foreach(string line in File.ReadAllLines(path))
         {
            if(line.StartsWith("["))
            {
               if(profileName != null)
               {
                  profiles[profileName] = profile;
                  profile = new Dictionary<string, string>();
               }

               profileName = line.Trim('[', ']');
            }
            else
            {
               string[] twoParts = line.Split(new[] { '=' }, 2, StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToArray();
               if(twoParts.Length == 2)
               {
                  profile[twoParts[0]] = twoParts[1];
               }
            }
         }

         if(profileName != null)
         {
            profiles[profileName] = profile;
         }

         return profiles;
      }

      private static string GetCredentialsPath()
      {
         string path = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".aws",
            "credentials");

         if(!File.Exists(path))
         {
            throw new IOException($"no credentials file found at {path}");
         }

         return path;
      }
   }
}
#endif