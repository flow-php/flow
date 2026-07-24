<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Psr\Http\Message\ResponseInterface;

use function explode;
use function str_ends_with;
use function strtolower;
use function trim;

final class ContentTypeDetector
{
    public static function detectFromResponse(ResponseInterface $response): ResponseType
    {
        foreach ($response->getHeader('Content-Type') as $header) {
            $type = self::detectFromHeader($header);

            if ($type !== null) {
                return $type;
            }
        }

        return ResponseType::TEXT;
    }

    public static function detectFromHeader(string $header): ?ResponseType
    {
        $mediaType = strtolower(trim(explode(';', $header, 2)[0]));

        return match (true) {
            $mediaType === 'application/json', str_ends_with($mediaType, '+json') => ResponseType::JSON,
            $mediaType === 'application/xml',
            $mediaType === 'text/xml',
            str_ends_with($mediaType, '+xml'),
                => ResponseType::XML,
            $mediaType === 'text/html' => ResponseType::HTML,
            default => null,
        };
    }
}
