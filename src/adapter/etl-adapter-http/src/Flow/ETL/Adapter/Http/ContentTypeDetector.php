<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

final class ContentTypeDetector
{
    /**
     * @param array<string> $headers
     */
    public static function detectFromHeaders(array $headers) : string
    {
        foreach ($headers as $header) {
            if (\str_contains($header, 'application/json')) {
                return 'json';
            }

            if (\str_contains($header, 'application/xml')) {
                return 'xml';
            }

            if (\str_contains($header, 'text/html')) {
                return 'html';
            }
        }

        return 'text';
    }
}
