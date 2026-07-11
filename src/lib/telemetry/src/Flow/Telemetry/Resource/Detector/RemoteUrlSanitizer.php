<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use function is_array;
use function parse_url;

/**
 * Reduces a remote URL to its scheme, host, optional port and path so that no
 * secrets are ever reported as a resource attribute. Embedded credentials
 * (user:password@) are removed, and any query string or fragment is dropped as
 * either can carry an access token.
 *
 * SCP-like SSH remotes (e.g. "git@github.com:org/repo.git") carry no secret and
 * are returned untouched, as are URLs that cannot be parsed.
 */
final readonly class RemoteUrlSanitizer
{
    public function sanitize(string $url): string
    {
        $parts = parse_url($url);

        if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])) {
            return $url;
        }

        $sanitized = $parts['scheme'] . '://' . $parts['host'];

        if (isset($parts['port'])) {
            $sanitized .= ':' . $parts['port'];
        }

        return $sanitized . ($parts['path'] ?? '');
    }
}
