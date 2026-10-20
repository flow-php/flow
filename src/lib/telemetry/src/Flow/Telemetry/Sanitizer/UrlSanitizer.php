<?php

declare(strict_types=1);

namespace Flow\Telemetry\Sanitizer;

use function in_array;

final readonly class UrlSanitizer
{
    public const array DEFAULT_SENSITIVE_QUERY_PARAMETERS = [
        // OpenTelemetry semantic conventions (url registry)
        'AWSAccessKeyId',
        'Signature',
        'sig',
        'X-Amz-Signature',
        'X-Amz-Credential',
        'X-Amz-Security-Token',
        'X-Goog-Signature',
        // OAuth 2.0 (RFC 6749, 6750, 7009, 7521, 7636, 8628, 8693) and OpenID Connect
        'access_token',
        'refresh_token',
        'id_token',
        'id_token_hint',
        'logout_token',
        'client_secret',
        'client_assertion',
        'assertion',
        'code',
        'code_verifier',
        'device_code',
        'subject_token',
        'actor_token',
        'token',
        // API keys, credentials and sessions (Sentry and Datadog default scrubbing lists)
        'api_key',
        'apikey',
        'api-key',
        'key',
        'access_key',
        'secret_key',
        'private_key',
        'secret',
        'signature',
        'password',
        'passwd',
        'pwd',
        'auth',
        'jwt',
        'session',
        'sessionid',
        'PHPSESSID',
    ];

    /**
     * @param list<string> $sensitiveQueryParameters full override of the default list
     */
    public function __construct(
        private array $sensitiveQueryParameters = self::DEFAULT_SENSITIVE_QUERY_PARAMETERS,
    ) {}

    public function message(string $message, string $url): string
    {
        return str_replace($url, $this->url($url), $message);
    }

    public function query(string $query): string
    {
        $pairs = [];

        foreach (explode('&', $query) as $pair) {
            $separatorPosition = strpos($pair, '=');

            if ($separatorPosition === false) {
                $pairs[] = $pair;

                continue;
            }

            $key = substr($pair, 0, $separatorPosition);

            $pairs[] = in_array(urldecode($key), $this->sensitiveQueryParameters, true) ? $key . '=REDACTED' : $pair;
        }

        return implode('&', $pairs);
    }

    public function url(string $url): string
    {
        $url = (string) preg_replace_callback(
            '~^([a-zA-Z][a-zA-Z0-9+.\-]*://)([^/?#@]*)@~',
            static fn(array $matches): string => (
                $matches[1] . (str_contains($matches[2], ':') ? 'REDACTED:REDACTED@' : 'REDACTED@')
            ),
            $url,
        );

        $fragmentPosition = strpos($url, '#');
        $fragment = $fragmentPosition === false ? '' : substr($url, $fragmentPosition);
        $withoutFragment = $fragmentPosition === false ? $url : substr($url, 0, $fragmentPosition);

        $queryPosition = strpos($withoutFragment, '?');

        if ($queryPosition === false) {
            return $url;
        }

        return (
            substr($withoutFragment, 0, $queryPosition + 1)
            . $this->query(substr($withoutFragment, $queryPosition + 1))
            . $fragment
        );
    }
}
