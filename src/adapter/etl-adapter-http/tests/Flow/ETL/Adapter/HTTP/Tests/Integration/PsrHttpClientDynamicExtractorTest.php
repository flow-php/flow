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
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
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

        // The row carries the body as raw text, so response_body holds one type whatever the
        // content type is; pagination reads the structured map through HttpEncoder::structuredBody().
        $body = type_array()->assert(json_decode(
            type_string()->assert($currentRows->first()->get('response_body')),
            true,
            512,
            JSON_THROW_ON_ERROR,
        ));

        static::assertSame(1, $currentRows->count());
        static::assertSame('flow-php', $body['login']);
        static::assertSame(73_495_297, $body['id']);

        $responseHeaders = type_array()->assert($currentRows->first()->get('response_headers'));
        static::assertSame(['GitHub.com'], $responseHeaders['Server']);
        static::assertSame(200, $currentRows->first()->get('response_status_code'));
        static::assertSame('1.1', $currentRows->first()->get('response_protocol_version'));
        static::assertSame('OK', $currentRows->first()->get('response_reason_phrase'));
        static::assertSame('https://api.github.com/orgs/flow-php', $currentRows->first()->get('request_uri'));
        static::assertSame('GET', $currentRows->first()->get('request_method'));
    }

    public function test_schema_is_the_fixed_http_exchange_shape(): void
    {
        static::assertEquals(
            schema(
                str_schema('response_body', nullable: true),
                map_schema('response_headers', type_map(type_string(), type_list(type_string()))),
                int_schema('response_status_code'),
                str_schema('response_protocol_version'),
                str_schema('response_reason_phrase'),
                str_schema('request_body', nullable: true),
                str_schema('request_uri'),
                map_schema('request_headers', type_map(type_string(), type_list(type_string()))),
                str_schema('request_protocol_version'),
                str_schema('request_method'),
            ),
            from_dynamic_http_requests(new Client(new Psr17Factory()), new class implements NextRequestFactory {
                public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
                {
                    return null;
                }
            })->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('response_body')),
            from_dynamic_http_requests(
                new Client(new Psr17Factory()),
                new class implements NextRequestFactory {
                    public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
                    {
                        return null;
                    }
                },
                schema(str_schema('response_body')),
            )->schema(),
        );
    }

    public function test_schema_typed_response_body_via_with_schema(): void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory);
        $psr18Client->addResponse(new Response(200, ['Content-Type' => 'application/json'], json_encode([
            'login' => 'flow-php',
            'id' => 73_495_297,
        ], JSON_THROW_ON_ERROR)));

        $extractor = from_dynamic_http_requests($psr18Client, new class implements NextRequestFactory {
            public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
            {
                return $previousResponse === null
                    ? (new Psr17Factory())->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                    : null;
            }
        })->withSchema(schema(structure_schema('response_body', type_structure([
            'login' => type_string(),
            'id' => type_integer(),
        ]))));

        $rows = $extractor->extract(flow_context(config()))->current();

        if (!$rows instanceof Rows) {
            static::fail('Expected Rows instance from extractor');
        }

        static::assertEquals(
            type_structure(['login' => type_string(), 'id' => type_integer()]),
            $rows->schema()->get('response_body')->type(),
        );
        static::assertSame(['login' => 'flow-php', 'id' => 73_495_297], $rows->first()->get('response_body'));
    }
}
