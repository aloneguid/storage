using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;

namespace Storage.Net.Tests.Integration.Utils
{
    public static class Utility
    {
        public static string CalculateMd5(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return CalculateMd5(stream);
            }
        }

        public static string CalculateMd5(Stream stream)
        {
            using (var md5 = MD5.Create())
            {
                var hash = md5.ComputeHash(stream);
                return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
            }
        }

        public static string CalculateMd5(string path)
        {
            using (var stream = File.OpenRead(path))
            {
                return CalculateMd5(stream);
            }
        }

        public static bool VerifyMd5Hash(string lHash, string rHash)
        {
            // Create a StringComparer an compare the hashes.
            StringComparer comparer = StringComparer.OrdinalIgnoreCase;
            return 0 == comparer.Compare(lHash, rHash);
        }

        public static void ClearTestData(string fileProjectRelativePath)
        {
            var fileName = fileProjectRelativePath.Split(Path.DirectorySeparatorChar).LastOrDefault();
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }

        public static void PrepareTestData(string fileProjectRelativePath)
        {
            var filePath = fileProjectRelativePath.Replace("/", @"\");
            var fileName = fileProjectRelativePath.Split(Path.DirectorySeparatorChar).LastOrDefault();

            var environmentDir = new DirectoryInfo(Environment.CurrentDirectory);

            var itemPathUri = new Uri(Path.Combine(environmentDir?.Parent?.Parent?.Parent?.FullName, filePath));
            var itemPath = itemPathUri.LocalPath;

            var binFolderPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var itemPathInBinUri = new Uri(Path.Combine(binFolderPath, fileName));
            var itemPathInBin = itemPathInBinUri.LocalPath;

            if (File.Exists(itemPath) && !File.Exists(itemPathInBin))
            {
                File.Copy(itemPath, itemPathInBin);
            }
        }

    }
}
