<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TracableHttpClient;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Contracts\HttpClient\ChunkInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Symfony\Contracts\HttpClient\ResponseStreamInterface;

#[CoversClass(TracableHttpClient::class)]
final class TracableHttpClientTest extends TestCase
{
    public function test_request_defaults_host_to_unknown_when_missing(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', '/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('unknown', $spans[0]->attributes()['server.address']);
    }

    public function test_request_defaults_scheme_to_http_when_missing(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', '/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('http', $spans[0]->attributes()['url.scheme']);
    }

    public function test_request_extracts_host_from_url(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('api.example.com', $spans[0]->attributes()['server.address']);
    }

    public function test_request_extracts_scheme_from_url(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = new class implements HttpClientInterface {
            /** @param array<string, mixed> $options */
            public function request(string $method, string $url, array $options = []): ResponseInterface
            {
                return new class implements ResponseInterface {
                    public function cancel(): void {}

                    public function getContent(bool $throw = true): string
                    {
                        return '';
                    }

                    /** @return array<string, list<null|string>> */
                    public function getHeaders(bool $throw = true): array
                    {
                        return [];
                    }

                    public function getInfo(?string $type = null): mixed
                    {
                        return null;
                    }

                    public function getStatusCode(): int
                    {
                        return 200;
                    }

                    /** @return array<string, mixed> */
                    public function toArray(bool $throw = true): array
                    {
                        return [];
                    }
                };
            }

            public function stream(
                ResponseInterface|iterable $responses,
                ?float $timeout = null,
            ): ResponseStreamInterface {
                throw new \RuntimeException('Not implemented');
            }

            /** @param array<string, mixed> $options */
            public function withOptions(array $options): static
            {
                return $this;
            }
        };

        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('https', $spans[0]->attributes()['url.scheme']);
    }

    public function test_request_includes_client_name_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'my_api_client');

        $tracable->request('GET', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('my_api_client', $spans[0]->attributes()['http.client.name']);
    }

    public function test_request_includes_http_status_code_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(200, $spans[0]->attributes()['http.response.status_code']);
    }

    public function test_request_includes_method_and_url_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('POST', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('POST', $spans[0]->attributes()['http.request.method']);
        static::assertSame('https://api.example.com/users', $spans[0]->attributes()['url.full']);
    }

    public function test_request_records_exception_on_failure(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = new class implements HttpClientInterface {
            /** @param array<string, mixed> $options */
            public function request(string $method, string $url, array $options = []): ResponseInterface
            {
                throw new \RuntimeException('Connection timeout');
            }

            public function stream(
                ResponseInterface|iterable $responses,
                ?float $timeout = null,
            ): ResponseStreamInterface {
                throw new \RuntimeException('Not implemented');
            }

            /** @param array<string, mixed> $options */
            public function withOptions(array $options): static
            {
                return $this;
            }
        };

        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $exceptionThrown = false;

        try {
            $tracable->request('GET', 'https://api.example.com/users');
        } catch (\RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $events = $spans[0]->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Connection timeout', $status->description);
    }

    public function test_request_sets_error_status_for_4xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(404);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/missing');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 404', $status->description);
    }

    public function test_request_sets_error_status_for_5xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(500);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/error');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 500', $status->description);
    }

    public function test_request_sets_ok_status_for_2xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(201);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('POST', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_request_sets_ok_status_for_3xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(302);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/redirect');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_request_span_name_includes_method_and_host(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('POST', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('POST api.example.com', $spans[0]->name());
    }

    public function test_span_kind_is_client(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $tracable->request('GET', 'https://api.example.com/users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
    }

    public function test_stream_delegates_to_inner_client(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockResponse = $this->createMock(ResponseInterface::class);
        $mockChunk = $this->createMock(ChunkInterface::class);

        $streamResponse = new readonly class($mockResponse, $mockChunk) implements ResponseStreamInterface {
            public function __construct(
                private ResponseInterface $response,
                private ChunkInterface $chunk,
            ) {}

            public function key(): ResponseInterface
            {
                return $this->response;
            }

            public function current(): ChunkInterface
            {
                return $this->chunk;
            }

            public function next(): void {}

            public function rewind(): void {}

            public function valid(): bool
            {
                return false;
            }
        };

        $innerClient = new readonly class($streamResponse) implements HttpClientInterface {
            public function __construct(
                private ResponseStreamInterface $stream,
            ) {}

            /** @param array<string, mixed> $options */
            public function request(string $method, string $url, array $options = []): ResponseInterface
            {
                throw new \RuntimeException('Not implemented');
            }

            public function stream(
                ResponseInterface|iterable $responses,
                ?float $timeout = null,
            ): ResponseStreamInterface {
                return $this->stream;
            }

            /** @param array<string, mixed> $options */
            public function withOptions(array $options): static
            {
                return $this;
            }
        };

        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $result = $tracable->stream($mockResponse);

        static::assertSame($streamResponse, $result);
    }

    public function test_with_options_creates_new_instance(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $innerClient = $this->createMockHttpClient(200);
        $tracable = new TracableHttpClient($innerClient, $telemetry, 'test.client');

        $newTracable = $tracable->withOptions(['timeout' => 30]);

        static::assertNotSame($tracable, $newTracable);
        static::assertInstanceOf(TracableHttpClient::class, $newTracable);
    }

    private function createMockHttpClient(int $statusCode): HttpClientInterface
    {
        return new readonly class($statusCode) implements HttpClientInterface {
            public function __construct(
                private int $statusCode,
            ) {}

            /** @param array<string, mixed> $options */
            public function request(string $method, string $url, array $options = []): ResponseInterface
            {
                $statusCode = $this->statusCode;

                return new readonly class($statusCode) implements ResponseInterface {
                    public function __construct(
                        private int $statusCode,
                    ) {}

                    public function cancel(): void {}

                    public function getContent(bool $throw = true): string
                    {
                        return '';
                    }

                    /** @return array<string, list<null|string>> */
                    public function getHeaders(bool $throw = true): array
                    {
                        return [];
                    }

                    public function getInfo(?string $type = null): mixed
                    {
                        return null;
                    }

                    public function getStatusCode(): int
                    {
                        return $this->statusCode;
                    }

                    /** @return array<string, mixed> */
                    public function toArray(bool $throw = true): array
                    {
                        return [];
                    }
                };
            }

            public function stream(
                ResponseInterface|iterable $responses,
                ?float $timeout = null,
            ): ResponseStreamInterface {
                throw new \RuntimeException('Not implemented');
            }

            /** @param array<string, mixed> $options */
            public function withOptions(array $options): static
            {
                return new self($this->statusCode);
            }
        };
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
