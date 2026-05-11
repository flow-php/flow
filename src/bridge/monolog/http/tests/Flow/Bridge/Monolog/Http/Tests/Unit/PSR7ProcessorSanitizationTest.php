<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit;

use Flow\Bridge\Monolog\Http\Config;
use Flow\Bridge\Monolog\Http\Config\RequestConfig;
use Flow\Bridge\Monolog\Http\Config\ResponseConfig;
use Flow\Bridge\Monolog\Http\PSR7Processor;
use Flow\ETL\Tests\FlowTestCase;
use Monolog\Level;
use Monolog\LogRecord;
use Nyholm\Psr7\Factory\Psr17Factory;

use function Flow\Bridge\Monolog\Http\DSL\mask;

final class PSR7ProcessorSanitizationTest extends FlowTestCase
{
    public function test_sanitizing_request_fields(): void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17
            ->createRequest('POST', 'https://example.com/api/v1/users')
            ->withHeader('User-Agent', 'Flow/1.0')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Authorization', 'Bearer token123')
            ->withBody($psr17->createStream(
                json_encode([
                    'username' => 'john_doe',
                    'password' => 'secret_password',
                    'email' => 'john@example.com',
                    'access_token' => 'sensitive_token',
                    'data' => [
                        'key' => 'sensitive_key',
                        'value' => 'public_value',
                    ],
                ]) ?: '{}',
            ));

        $processor = new PSR7Processor(new Config(new RequestConfig(withBody: true, sanitizers: [
            'password' => mask(),
            'access_token' => mask('#'),
            'key' => mask('*', 2),
        ])));

        $record = $processor(new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'http',
            level: Level::Debug,
            message: 'HTTP Request',
            context: ['request' => $request],
        ));

        /** @phpstan-ignore-next-line */
        $requestData = json_decode((string) $record->context['request']['body'], true);
        \assert(\is_array($requestData));

        static::assertEquals('john_doe', $requestData['username']);
        static::assertEquals('***************', $requestData['password']);
        static::assertEquals('john@example.com', $requestData['email']);
        static::assertEquals('###############', $requestData['access_token']);
        \assert(\is_array($requestData['data']));
        static::assertEquals('se***********', $requestData['data']['key']);
        static::assertEquals('public_value', $requestData['data']['value']);
    }

    public function test_sanitizing_response_fields(): void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17
            ->createResponse(200)
            ->withHeader('Content-Type', 'application/json')
            ->withBody($psr17->createStream(
                json_encode([
                    'status' => 'success',
                    'data' => [
                        'user' => [
                            'id' => 123,
                            'username' => 'john_doe',
                            'credentials' => 'sensitive_credentials',
                            'access_token' => 'sensitive_token',
                        ],
                    ],
                ]) ?: '{}',
            ));

        $processor = new PSR7Processor(new Config(
            request: new RequestConfig(),
            response: new ResponseConfig(withBody: true, sanitizers: [
                'credentials' => ['type' => 'mask', 'character' => '*', 'offset' => 0],
                'access_token' => mask('#', 3),
            ]),
        ));

        $record = $processor(new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'http',
            level: Level::Debug,
            message: 'HTTP Response',
            context: ['response' => $response],
        ));

        /** @phpstan-ignore-next-line */
        $responseData = json_decode((string) $record->context['response']['body'], true);
        \assert(\is_array($responseData));

        static::assertEquals('success', $responseData['status']);
        \assert(\is_array($responseData['data']));
        \assert(\is_array($responseData['data']['user']));
        static::assertEquals(123, $responseData['data']['user']['id']);
        static::assertEquals('john_doe', $responseData['data']['user']['username']);
        static::assertEquals('*********************', $responseData['data']['user']['credentials']);
        static::assertEquals('sen############', $responseData['data']['user']['access_token']);
    }
}
