<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit;

use Flow\Bridge\Monolog\Http\Config\{RequestConfig, ResponseConfig};
use Flow\Bridge\Monolog\Http\{Config, PSR7Processor};
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Factory\Psr17Factory;

final class PSR7ProcessorTest extends FlowTestCase
{
    public function test_normalizing_http_request() : void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17->createRequest('GET', 'https://example.com/api/v1/users?limit=10#page=1')
            ->withHeader('User-Agent', 'Flow/1.0')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Authorization', 'Bearer 123')
            ->withBody($psr17->createStream('Hello World!'));

        $processor = new PSR7Processor();

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Request', 'context' => ['request' => $request]]);

        self::assertEquals(
            [
                'request' => [
                    'method' => 'GET',
                    'uri' => 'https://example.com/api/v1/users?limit=10#page=1',
                    'headers' => [
                        'User-Agent' => ['Flow/1.0'],
                        'Host' => ['example.com'],
                    ],
                ],
            ],
            $record['context'],
        );
    }

    public function test_normalizing_http_request_with_body() : void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17->createRequest('POST', 'https://example.com/api/v1/users')
            ->withHeader('User-Agent', 'Flow/1.0')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Authorization', 'Bearer 123')
            ->withBody($psr17->createStream('Hello World!'));

        $processor = new PSR7Processor((new Config(new RequestConfig(withBody: true))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Request', 'context' => ['request' => $request]]);

        self::assertEquals(
            [
                'request' => [
                    'method' => 'POST',
                    'uri' => 'https://example.com/api/v1/users',
                    'headers' => [
                        'User-Agent' => ['Flow/1.0'],
                        'Host' => ['example.com'],
                    ],
                    'body' => 'Hello World!',
                ],
            ],
            $record['context'],
        );
    }

    public function test_normalizing_http_request_with_limited_body() : void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17->createRequest('POST', 'https://example.com/api/v1/users')
            ->withHeader('User-Agent', 'Flow/1.0')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Authorization', 'Bearer 123')
            ->withBody($psr17->createStream('Hello World!'));

        $processor = new PSR7Processor((new Config(new RequestConfig(withBody: true, bodySizeLimit: 5))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Request', 'context' => ['request' => $request]]);

        self::assertEquals(
            [
                'request' => [
                    'method' => 'POST',
                    'uri' => 'https://example.com/api/v1/users',
                    'headers' => [
                        'User-Agent' => ['Flow/1.0'],
                        'Host' => ['example.com'],
                    ],
                    'body' => 'Hello',
                ],
            ],
            $record['context'],
        );
    }

    public function test_normalizing_http_request_without_headers() : void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17->createRequest('POST', 'https://example.com/api/v1/users')
            ->withBody($psr17->createStream('Hello World!'));

        $processor = new PSR7Processor((new Config(new RequestConfig(headers: []))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Request', 'context' => ['request' => $request]]);

        self::assertEquals(
            [
                'request' => [
                    'method' => 'POST',
                    'uri' => 'https://example.com/api/v1/users',
                ],
            ],
            $record['context'],
        );
    }

    public function test_normalizing_http_response() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(200, 'OK')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Content-Length', '42')
            ->withBody($psr17->createStream('{"message":"Hello, World!"}'));

        $processor = new PSR7Processor();

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        self::assertEquals(
            [
                'response' => [
                    'status' => 200,
                    'reason_phrase' => 'OK',
                    'headers' => [
                        'Content-Type' => ['application/json'],
                        'Content-Length' => ['42'],
                    ],
                ],
            ],
            $record['context']
        );
    }

    public function test_normalizing_http_response_when_status_code_is_excluded() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(404, 'Not Found')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Content-Length', '42')
            ->withBody($psr17->createStream('{"message":"Not Found!"}'));

        $processor = new PSR7Processor((new Config(response: new ResponseConfig(withoutStatusCodes: [404]))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        self::assertEquals([], $record['context']);
    }

    public function test_normalizing_http_response_with_body() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(200, 'OK')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Content-Length', '42')
            ->withBody($psr17->createStream('{"message":"Hello, World!"}'));

        $processor = new PSR7Processor((new Config(response: new ResponseConfig(withBody: true))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        self::assertEquals(
            [
                'response' => [
                    'status' => 200,
                    'reason_phrase' => 'OK',
                    'headers' => [
                        'Content-Type' => ['application/json'],
                        'Content-Length' => ['42'],
                    ],
                    'body' => '{"message":"Hello, World!"}',
                ],
            ],
            $record['context']
        );
    }

    public function test_normalizing_http_response_with_body_limit() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(200, 'OK')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Content-Length', '42')
            ->withBody($psr17->createStream('{"message":"Hello, World!"}'));

        $processor = new PSR7Processor((new Config(response: new ResponseConfig(withBody: true, bodySizeLimit: 5))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        self::assertEquals(
            [
                'response' => [
                    'status' => 200,
                    'reason_phrase' => 'OK',
                    'headers' => [
                        'Content-Type' => ['application/json'],
                        'Content-Length' => ['42'],
                    ],
                    'body' => '{"mes',
                ],
            ],
            $record['context']
        );
    }

    public function test_normalizing_http_response_without_available_body() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(200, 'OK')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Content-Length', '42');

        $processor = new PSR7Processor((new Config(response: new ResponseConfig(withBody: true))));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        self::assertEquals(
            [
                'response' => [
                    'status' => 200,
                    'reason_phrase' => 'OK',
                    'headers' => [
                        'Content-Type' => ['application/json'],
                        'Content-Length' => ['42'],
                    ],
                ],
            ],
            $record['context']
        );
    }
}
