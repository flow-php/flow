<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Tests\Unit;

use function Flow\Bridge\Monolog\Http\DSL\mask;
use Flow\Bridge\Monolog\Http\Config\{RequestConfig, ResponseConfig};
use Flow\Bridge\Monolog\Http\{Config, PSR7Processor};
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Factory\Psr17Factory;

final class PSR7ProcessorSanitizationTest extends FlowTestCase
{
    public function test_sanitizing_request_fields() : void
    {
        $psr17 = new Psr17Factory();

        $request = $psr17->createRequest('POST', 'https://example.com/api/v1/users')
            ->withHeader('User-Agent', 'Flow/1.0')
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('Authorization', 'Bearer token123')
            ->withBody($psr17->createStream(json_encode([
                'username' => 'john_doe',
                'password' => 'secret_password',
                'email' => 'john@example.com',
                'access_token' => 'sensitive_token',
                'data' => [
                    'key' => 'sensitive_key',
                    'value' => 'public_value',
                ],
            ]) ?: '{}'));

        $processor = new PSR7Processor(new Config(
            new RequestConfig(
                withBody: true,
                sanitizers: [
                    'password' => mask(),
                    'access_token' => mask('#'),
                    'key' => mask('*', 2),
                ]
            )
        ));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Request', 'context' => ['request' => $request]]);

        $requestData = json_decode((string) $record['context']['request']['body'], true);

        self::assertEquals('john_doe', $requestData['username']);
        self::assertEquals('***************', $requestData['password']);
        self::assertEquals('john@example.com', $requestData['email']);
        self::assertEquals('###############', $requestData['access_token']);
        self::assertEquals('se***********', $requestData['data']['key']);
        self::assertEquals('public_value', $requestData['data']['value']);
    }

    public function test_sanitizing_response_fields() : void
    {
        $psr17 = new Psr17Factory();

        $response = $psr17->createResponse(200)
            ->withHeader('Content-Type', 'application/json')
            ->withBody($psr17->createStream(json_encode([
                'status' => 'success',
                'data' => [
                    'user' => [
                        'id' => 123,
                        'username' => 'john_doe',
                        'credentials' => 'sensitive_credentials',
                        'access_token' => 'sensitive_token',
                    ],
                ],
            ]) ?: '{}'));

        $processor = new PSR7Processor(new Config(
            request: new RequestConfig(),
            response: new ResponseConfig(
                withBody: true,
                sanitizers: [
                    'credentials' => ['type' => 'mask', 'character' => '*', 'offset' => 0],
                    'access_token' => mask('#', 3),
                ]
            )
        ));

        $record = $processor(['datetime' => new \DateTimeImmutable, 'channel' => 'http', 'level_name' => 'debug', 'message' => 'HTTP Response', 'context' => ['response' => $response]]);

        $responseData = json_decode((string) $record['context']['response']['body'], true);

        self::assertEquals('success', $responseData['status']);
        self::assertEquals(123, $responseData['data']['user']['id']);
        self::assertEquals('john_doe', $responseData['data']['user']['username']);
        self::assertEquals('*********************', $responseData['data']['user']['credentials']);
        self::assertEquals('sen############', $responseData['data']['user']['access_token']);
    }
}
