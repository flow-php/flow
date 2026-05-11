<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Psr\Http\Message\ResponseInterface;

final class ContentTypeDetector
{
    public static function detectFromResponse(ResponseInterface $response): ResponseType
    {
        foreach ($response->getHeader('Content-Type') as $header) {
            if (\str_contains($header, 'application/json')) {
                return ResponseType::JSON;
            }

            if (\str_contains($header, 'application/xml')) {
                return ResponseType::XML;
            }

            if (\str_contains($header, 'text/html')) {
                return ResponseType::HTML;
            }
        }

        return ResponseType::TEXT;
    }
}
