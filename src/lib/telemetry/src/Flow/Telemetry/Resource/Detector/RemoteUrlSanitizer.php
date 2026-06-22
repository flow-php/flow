<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use function is_array;
use function parse_url;

/**
 * Removes any embedded credentials (user:password@) from a remote URL so they
 * are never reported as a resource attribute.
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

        if (!isset($parts['user']) && !isset($parts['pass'])) {
            return $url;
        }

        $sanitized = $parts['scheme'] . '://' . $parts['host'];

        if (isset($parts['port'])) {
            $sanitized .= ':' . $parts['port'];
        }

        $sanitized .= $parts['path'] ?? '';

        if (isset($parts['query'])) {
            $sanitized .= '?' . $parts['query'];
        }

        if (isset($parts['fragment'])) {
            $sanitized .= '#' . $parts['fragment'];
        }

        return $sanitized;
    }
}
