namespace NServiceBus.RavenDB.Internal
{
	using System;

	internal static class RavenIdUtil
	{
		internal static Guid HashString(string valueToHash)
		{
			// use MD5 hash to get a 16-byte hash of the string

			var endpointHash = Raven.Abstractions.Util.MD5Core.GetHash(valueToHash);

			//            using (var provider = new MD5CryptoServiceProvider())
			{
				//var inputBytes = Encoding.Default.GetBytes(messageType.TypeName + "/" + messageType.Version.Major);
				//var hashBytes = provider.ComputeHash(inputBytes);
				// generate a guid from the hash:
				//var id = new Guid(hashBytes);
				return new Guid(endpointHash);
			}
		}

		internal static Guid HashBytes(byte[] valueToHash)
		{
			var endpointHash = Raven.Abstractions.Util.MD5Core.GetHash(valueToHash);
			{
				return new Guid(endpointHash);
			}
		}

	}
}