<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\CastingException;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use RuntimeException;
use Stringable;

use function file_get_contents;
use function Flow\ETL\Adapter\Http\from_static_http_requests;
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
use function is_scalar;
use function json_decode;
use function json_encode;

final class PsrHttpClientStaticExtractorTest extends FlowTestCase
{
    public function test_http_extractor(): void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory);
        $norbertFixture = file_get_contents(__DIR__ . '/../Fixtures/norberttech.json');

        if ($norbertFixture === false) {
            throw new RuntimeException('Failed to read norberttech fixture file');
        }

        $tomaszFixture = file_get_contents(__DIR__ . '/../Fixtures/tomaszhanc.json');

        if ($tomaszFixture === false) {
            throw new RuntimeException('Failed to read tomaszhanc fixture file');
        }

        $psr18Client->addResponse(new Response(200, [], $norbertFixture));
        $psr18Client->addResponse(new Response(200, [], $tomaszFixture));

        $requests = [
            $psr17Factory
                ->createRequest('GET', 'https://api.github.com/users/norberttech')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl'),
            $psr17Factory
                ->createRequest('GET', 'https://api.github.com/users/tomaszhanc')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl'),
        ];

        $extractor = from_static_http_requests($psr18Client, $requests);

        $rowsGenerator = $extractor->extract(flow_context(config()));

        $norbertRows = $rowsGenerator->current();

        if (!$norbertRows instanceof Rows) {
            static::fail('Expected Rows instance for norberttech');
        }

        $rowsGenerator->next();

        $tomekRows = $rowsGenerator->current();

        if (!$tomekRows instanceof Rows) {
            static::fail('Expected Rows instance for tomaszhanc');
        }

        $norbertResponseBodyValue = $norbertRows->first()->get('response_body');
        $norbertBodyJson = is_scalar($norbertResponseBodyValue) || $norbertResponseBodyValue instanceof Stringable
            ? (string) $norbertResponseBodyValue
            : '';
        $norbertResponseBody = type_array()->assert(json_decode($norbertBodyJson, true, 512, JSON_THROW_ON_ERROR));

        $tomekResponseBodyValue = $tomekRows->first()->get('response_body');
        $tomekBodyJson = is_scalar($tomekResponseBodyValue) || $tomekResponseBodyValue instanceof Stringable
            ? (string) $tomekResponseBodyValue
            : '';
        $tomekResponseBody = type_array()->assert(json_decode($tomekBodyJson, true, 512, JSON_THROW_ON_ERROR));

        static::assertSame('norberttech', $norbertResponseBody['login']);
        static::assertSame('tomaszhanc', $tomekResponseBody['login']);
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
            from_static_http_requests(new Client(new Psr17Factory()), [])->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('response_body')),
            from_static_http_requests(
                new Client(new Psr17Factory()),
                [],
                schema(str_schema('response_body')),
            )->schema(),
        );
    }

    public function test_schema_typed_response_body_with_empty_body(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(new Response(200, ['Content-Type' => 'application/json'], '{}'));

        $this->expectException(CastingException::class);

        from_static_http_requests(
            $client,
            [$factory->createRequest('GET', 'https://api.github.com/users/norberttech')],
            schema(structure_schema('response_body', type_structure([
                'login' => type_string(),
                'id' => type_integer(),
            ]))),
        )
            ->extract(flow_context(config()))
            ->current();
    }

    public function test_schema_typed_response_body_with_missing_field(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(new Response(200, ['Content-Type' => 'application/json'], json_encode([
            'login' => 'norberttech',
        ], JSON_THROW_ON_ERROR)));

        $this->expectException(CastingException::class);

        from_static_http_requests(
            $client,
            [$factory->createRequest('GET', 'https://api.github.com/users/norberttech')],
            schema(structure_schema('response_body', type_structure([
                'login' => type_string(),
                'id' => type_integer(),
            ]))),
        )
            ->extract(flow_context(config()))
            ->current();
    }

    public function test_schema_typed_response_body(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(new Response(200, ['Content-Type' => 'application/json'], json_encode([
            'login' => 'norberttech',
            'id' => 1,
        ], JSON_THROW_ON_ERROR)));

        $rows = from_static_http_requests(
            $client,
            [$factory->createRequest('GET', 'https://api.github.com/users/norberttech')],
            schema(structure_schema('response_body', type_structure([
                'login' => type_string(),
                'id' => type_integer(),
            ]))),
        )
            ->extract(flow_context(config()))
            ->current();

        if (!$rows instanceof Rows) {
            static::fail('Expected Rows instance');
        }

        static::assertEquals(
            type_structure(['login' => type_string(), 'id' => type_integer()]),
            $rows->schema()->get('response_body')->type(),
        );
        static::assertSame(['login' => 'norberttech', 'id' => 1], $rows->first()->get('response_body'));
    }
}
