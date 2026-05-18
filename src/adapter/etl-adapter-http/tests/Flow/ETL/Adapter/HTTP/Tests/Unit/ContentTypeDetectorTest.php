<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit;

use Flow\ETL\Adapter\Http\ContentTypeDetector;
use Flow\ETL\Adapter\Http\ResponseType;
use Generator;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

final class ContentTypeDetectorTest extends TestCase
{
    public static function responses(): Generator
    {
        yield 'application/json' => [
            new Response(headers: ['Content-Type' => 'application/json']),
            ResponseType::JSON,
        ];

        yield 'application/xml' => [
            new Response(headers: ['Content-Type' => 'application/xml']),
            ResponseType::XML,
        ];

        yield 'text/html' => [
            new Response(headers: ['Content-Type' => 'text/html']),
            ResponseType::HTML,
        ];

        yield 'unknown' => [
            new Response(headers: ['Content-Type' => 'unknown']),
            ResponseType::TEXT,
        ];

        yield 'missing' => [
            new Response(),
            ResponseType::TEXT,
        ];
    }

    #[DataProvider('responses')]
    public function test_detects_content_type_from_response(ResponseInterface $response, ResponseType $expected): void
    {
        static::assertSame($expected, ContentTypeDetector::detectFromResponse($response));
    }
}
