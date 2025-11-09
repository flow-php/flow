<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Psr\Http\Message\ResponseInterface;

final class ContentTypeDetector
{
    public static function detectFromHeaders(ResponseInterface $response) : string
    {
        foreach ($response->getHeader('Content-Type') as $header) {
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
