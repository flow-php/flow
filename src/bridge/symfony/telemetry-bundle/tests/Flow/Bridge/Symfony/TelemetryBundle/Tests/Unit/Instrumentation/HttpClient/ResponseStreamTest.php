<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\ResponseStream;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\Chunk;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\MockResponse;
use Generator;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(ResponseStream::class)]
final class ResponseStreamTest extends TestCase
{
    public function test_delegates_iterator_methods_to_generator(): void
    {
        $response = new MockResponse(200);
        $chunk = new Chunk(isLast: true);

        $stream = new ResponseStream(
            (static function () use ($response, $chunk): Generator {
                yield $response => $chunk;
            })(),
        );

        $stream->rewind();

        static::assertTrue($stream->valid());
        static::assertSame($response, $stream->key());
        static::assertSame($chunk, $stream->current());

        $stream->next();

        static::assertFalse($stream->valid());
    }
}
