<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination;

enum SensitiveHeaders: string
{
    case ApiKey = 'Api-Key';
    case Authorization = 'Authorization';
    case Cookie = 'Cookie';
    case DatadogApiKey = 'DD-API-KEY';
    case DatadogApplicationKey = 'DD-APPLICATION-KEY';
    case GitlabJobToken = 'JOB-TOKEN';
    case GitlabPrivateToken = 'PRIVATE-TOKEN';
    case OcpApimSubscriptionKey = 'Ocp-Apim-Subscription-Key';
    case ProxyAuthorization = 'Proxy-Authorization';
    case XAccessToken = 'X-Access-Token';
    case XAmzSecurityToken = 'X-Amz-Security-Token';
    case XApiKey = 'X-Api-Key';
    case XApiToken = 'X-Api-Token';
    case XAuthEmail = 'X-Auth-Email';
    case XAuthKey = 'X-Auth-Key';
    case XAuthToken = 'X-Auth-Token';
    case XCsrfToken = 'X-CSRF-Token';
    case XFunctionsKey = 'X-Functions-Key';
    case XGoogApiKey = 'X-Goog-Api-Key';
    case XShopifyAccessToken = 'X-Shopify-Access-Token';
    case XVaultToken = 'X-Vault-Token';
    case XXsrfToken = 'X-XSRF-Token';

    /**
     * @return list<string>
     */
    public static function toArray(): array
    {
        return array_map(static fn(self $header): string => $header->value, self::cases());
    }
}
