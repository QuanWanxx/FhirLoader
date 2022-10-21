using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QuwanLoader;

namespace FhirLoader.QuwanLoader
{
    /// <summary>
    /// Provide access token for the given FHIR url from token credential.
    /// Access token will be cached and will be refreshed if expired.
    /// </summary>
    public class FhirAccessTokenProvider
    {
        private readonly TokenCredential _tokenCredential;
        private ConcurrentDictionary<string, AccessToken> _accessTokenDic = new ();
        private const int _tokenExpireInterval = 3;
        private object _lock = new object ();
        ILogger<FhirAccessTokenProvider> _logger;

        public FhirAccessTokenProvider(
            IOptions<UploadConfiguration> config,
            ILogger<FhirAccessTokenProvider> logger)
        {
            _logger = logger;
            if (!string.IsNullOrEmpty(config.Value.TenantId)
                && !string.IsNullOrEmpty(config.Value.ClientSecret)
                && !string.IsNullOrEmpty(config.Value.ClientId))
            {
                _logger.LogInformation("Using client secret credential.");
                _tokenCredential = new ClientSecretCredential(config.Value.TenantId, config.Value.ClientId, config.Value.ClientSecret);
            }
            else
            {
                _logger.LogInformation("Using default azure credential.");
                _tokenCredential = new DefaultAzureCredential();
            }
        }

        public async Task<string> GetAccessTokenAsync(string resourceUrl, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!_accessTokenDic.TryGetValue(resourceUrl, out AccessToken accessToken) || string.IsNullOrEmpty(accessToken.Token) || accessToken.ExpiresOn < DateTimeOffset.UtcNow.AddMinutes(_tokenExpireInterval))
                {
                    lock (_lock)
                    {
                        _logger.LogInformation("Entering lock, try to refresh token.");
                        if (!_accessTokenDic.TryGetValue(resourceUrl, out AccessToken accessToken2) || string.IsNullOrEmpty(accessToken2.Token) || accessToken2.ExpiresOn < DateTimeOffset.UtcNow.AddMinutes(_tokenExpireInterval))
                        {
                            var scopes = new string[] { resourceUrl.TrimEnd('/') + "/.default" };
                            accessToken = _tokenCredential.GetToken(new TokenRequestContext(scopes), cancellationToken);
                            _accessTokenDic.AddOrUpdate(resourceUrl, accessToken, (key, value) => accessToken);

                            _logger.LogInformation($"Entering lock, acquired refresh token. expires at {accessToken.ExpiresOn}");

                            return accessToken.Token;
                        }

                        _logger.LogInformation("Entering lock, return token.");
                        return accessToken2.Token;
                    }
                }
                
                return accessToken.Token;
            }
            catch (Exception exception)
            {
                _logger.LogError("Get access token for resource '{0}' failed. Reason: '{1}'", resourceUrl, exception);
                throw;
            }
        }
    }
}