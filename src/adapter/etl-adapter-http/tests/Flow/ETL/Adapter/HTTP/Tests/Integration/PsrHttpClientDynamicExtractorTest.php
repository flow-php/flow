<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;
use Stringable;

use function assert;
use function file_get_contents;
use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function is_array;
use function is_scalar;
use function json_decode;
use function json_encode;

final class PsrHttpClientDynamicExtractorTest extends FlowTestCase
{
    public function test_http_extractor(): void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory);

        $fixtureContent = file_get_contents(__DIR__ . '/../Fixtures/flow-php.json');

        if ($fixtureContent === false) {
            throw new RuntimeException('Failed to read fixture file');
        }

        $psr18Client->addResponse(
            new Response(
                200,
                [
                    'Server' => 'GitHub.com',
                ],
                $fixtureContent,
            ),
        );

        $extractor = from_dynamic_http_requests($psr18Client, new class implements NextRequestFactory {
            public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
            {
                $psr17Factory = new Psr17Factory();

                if ($previousResponse === null) {
                    return $psr17Factory
                        ->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                        ->withHeader('Accept', 'application/vnd.github.v3+json')
                        ->withHeader('User-Agent', 'flow-php/etl');
                }

                return null;
            }
        });

        $rows = $extractor->extract(flow_context(config()));

        $responseBody = $rows->current()->first()->valueOf('response_body');
        $bodyJson = is_scalar($responseBody) || $responseBody instanceof Stringable ? (string) $responseBody : '';
        $body = json_decode($bodyJson, true, 512, JSON_THROW_ON_ERROR);
        assert(is_array($body));

        static::assertSame(1, $rows->current()->count());
        static::assertSame('flow-php', $body['login'], json_encode($body, JSON_THROW_ON_ERROR));
        static::assertSame(73_495_297, $body['id'], json_encode($body, JSON_THROW_ON_ERROR));

        $responseHeadersValue = $rows->current()->first()->valueOf('response_headers');
        static::assertInstanceOf(Json::class, $responseHeadersValue);
        $responseHeaders = $responseHeadersValue->toArray();
        static::assertSame(['GitHub.com'], $responseHeaders['Server']);
        static::assertSame(200, $rows->current()->first()->valueOf('response_status_code'));
        static::assertSame('1.1', $rows->current()->first()->valueOf('response_protocol_version'));
        static::assertSame('OK', $rows->current()->first()->valueOf('response_reason_phrase'));
        static::assertSame('https://api.github.com/orgs/flow-php', $rows->current()->first()->valueOf('request_uri'));
        static::assertSame('GET', $rows->current()->first()->valueOf('request_method'));
    }
}
