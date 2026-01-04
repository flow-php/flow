<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_http_transport, otlp_json_serializer};
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\HttpTransport;
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind};
use Flow\Telemetry\Transport\TransportException;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;

use Psr\Http\Message\{RequestFactoryInterface, RequestInterface, ResponseInterface, StreamFactoryInterface, StreamInterface};

final class HttpTransportTest extends TestCase
{
    public function test_send_creates_correct_request() : void
    {
        $stream = $this->createMock(StreamInterface::class);
        $streamFactory = $this->createMock(StreamFactoryInterface::class);
        $streamFactory->method('createStream')
            ->willReturn($stream);

        $request = $this->createMock(RequestInterface::class);
        $request->method('withHeader')
            ->willReturnSelf();
        $request->method('withBody')
            ->willReturnSelf();

        $requestFactory = $this->createMock(RequestFactoryInterface::class);
        $requestFactory->expects(self::once())
            ->method('createRequest')
            ->with('POST', 'http://localhost:4318/v1/traces')
            ->willReturn($request);

        $response = $this->createMock(ResponseInterface::class);
        $response->method('getStatusCode')
            ->willReturn(200);

        $client = $this->createMock(ClientInterface::class);
        $client->expects(self::once())
            ->method('sendRequest')
            ->willReturn($response);

        $transport = otlp_http_transport($client, $requestFactory, $streamFactory, 'http://localhost:4318', otlp_json_serializer());
        $transport->sendSpans($this->createSpans());
    }

    public function test_send_includes_custom_headers() : void
    {
        $stream = $this->createMock(StreamInterface::class);
        $streamFactory = $this->createMock(StreamFactoryInterface::class);
        $streamFactory->method('createStream')
            ->willReturn($stream);

        $request = $this->createMock(RequestInterface::class);
        $request->expects(self::exactly(2))
            ->method('withHeader')
            ->willReturnSelf();
        $request->method('withBody')
            ->willReturnSelf();

        $requestFactory = $this->createMock(RequestFactoryInterface::class);
        $requestFactory->method('createRequest')
            ->willReturn($request);

        $response = $this->createMock(ResponseInterface::class);
        $response->method('getStatusCode')
            ->willReturn(200);

        $client = $this->createMock(ClientInterface::class);
        $client->method('sendRequest')
            ->willReturn($response);

        $transport = new HttpTransport(
            $client,
            $requestFactory,
            $streamFactory,
            'http://localhost:4318',
            new JsonSerializer(),
            ['Authorization' => 'Bearer token'],
        );
        $transport->sendSpans($this->createSpans());
    }

    public function test_send_throws_on_http_error() : void
    {
        $stream = $this->createMock(StreamInterface::class);
        $streamFactory = $this->createMock(StreamFactoryInterface::class);
        $streamFactory->method('createStream')
            ->willReturn($stream);

        $request = $this->createMock(RequestInterface::class);
        $request->method('withHeader')
            ->willReturnSelf();
        $request->method('withBody')
            ->willReturnSelf();

        $requestFactory = $this->createMock(RequestFactoryInterface::class);
        $requestFactory->method('createRequest')
            ->willReturn($request);

        $responseBody = $this->createMock(StreamInterface::class);
        $responseBody->method('__toString')
            ->willReturn('Internal Server Error');

        $response = $this->createMock(ResponseInterface::class);
        $response->method('getStatusCode')
            ->willReturn(500);
        $response->method('getBody')
            ->willReturn($responseBody);

        $client = $this->createMock(ClientInterface::class);
        $client->method('sendRequest')
            ->willReturn($response);

        $transport = otlp_http_transport($client, $requestFactory, $streamFactory, 'http://localhost:4318', otlp_json_serializer());

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('HTTP 500');

        $transport->sendSpans($this->createSpans());
    }

    public function test_shutdown_does_nothing() : void
    {
        $client = $this->createMock(ClientInterface::class);
        $requestFactory = $this->createMock(RequestFactoryInterface::class);
        $streamFactory = $this->createMock(StreamFactoryInterface::class);

        $transport = otlp_http_transport($client, $requestFactory, $streamFactory, 'http://localhost:4318', otlp_json_serializer());
        $transport->shutdown();

        $this->addToAssertionCount(1);
    }

    /**
     * @return array<Span>
     */
    private function createSpans() : array
    {
        return [new Span(
            'test-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        )];
    }
}
