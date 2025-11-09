<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit;

use Flow\ETL\Adapter\Http\ContentTypeDetector;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

final class ContentTypeDetectorTest extends TestCase
{
    public static function responses() : \Generator
    {
        yield 'application/json' => [
            new Response(headers: ['Content-Type' => 'application/json']),
            'json',
        ];

        yield 'application/xml' => [
            new Response(headers: ['Content-Type' => 'application/xml']),
            'xml',
        ];

        yield 'text/html' => [
            new Response(headers: ['Content-Type' => 'text/html']),
            'html',
        ];

        yield 'unknown' => [
            new Response(headers: ['Content-Type' => 'unknown']),
            'text',
        ];

        yield 'missing' => [
            new Response(),
            'text',
        ];
    }

    #[DataProvider('responses')]
    public function test_detects_content_type_from_response(ResponseInterface $response, string $expected) : void
    {
        self::assertSame(
            $expected,
            ContentTypeDetector::detectFromResponse($response),
        );
    }
}
