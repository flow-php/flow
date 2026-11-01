<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use function is_array;
use function parse_url;

/**
 * SCP-like and unparsable URLs are returned untouched.
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
