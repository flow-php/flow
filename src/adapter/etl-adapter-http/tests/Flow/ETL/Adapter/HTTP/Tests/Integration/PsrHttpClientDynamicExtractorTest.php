<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;

use function file_get_contents;
use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\Types\DSL\type_array;

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
                    'Content-Type' => 'application/json',
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

        $generator = $extractor->extract(flow_context(config()));

        $currentRows = $generator->current();

        if (!$currentRows instanceof Rows) {
            static::fail('Expected Rows instance from extractor');
        }

        $body = type_array()->assert($currentRows->first()->valueOf('response_body'));

        static::assertSame(1, $currentRows->count());
        static::assertSame('flow-php', $body['login']);
        static::assertSame(73_495_297, $body['id']);

        $responseHeaders = type_array()->assert($currentRows->first()->valueOf('response_headers'));
        static::assertSame(['GitHub.com'], $responseHeaders['Server']);
        static::assertSame(200, $currentRows->first()->valueOf('response_status_code'));
        static::assertSame('1.1', $currentRows->first()->valueOf('response_protocol_version'));
        static::assertSame('OK', $currentRows->first()->valueOf('response_reason_phrase'));
        static::assertSame('https://api.github.com/orgs/flow-php', $currentRows->first()->valueOf('request_uri'));
        static::assertSame('GET', $currentRows->first()->valueOf('request_method'));
    }
}
