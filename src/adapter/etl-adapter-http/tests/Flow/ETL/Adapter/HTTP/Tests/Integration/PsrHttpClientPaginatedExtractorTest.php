<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use Psr\Http\Message\RequestInterface;

use function array_map;
use function Flow\ETL\Adapter\Http\from_http_paginated;
use function Flow\ETL\Adapter\Http\http_pagination_cursor;
use function Flow\ETL\Adapter\Http\http_pagination_last_record_cursor;
use function Flow\ETL\Adapter\Http\http_pagination_link_header;
use function Flow\ETL\Adapter\Http\http_pagination_next_url;
use function Flow\ETL\Adapter\Http\http_pagination_offset;
use function Flow\ETL\Adapter\Http\http_pagination_page_number;
use function Flow\ETL\Adapter\Http\http_request_option_body;
use function Flow\ETL\Adapter\Http\http_request_option_query;
use function Flow\ETL\Adapter\Http\http_stop_when_max_pages;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;
use function json_decode;

final class PsrHttpClientPaginatedExtractorTest extends FlowTestCase
{
    public function test_body_path_injection_on_post_request(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c2'], 'data' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => null], 'data' => [1]]));

        $base = $factory
            ->createRequest('POST', 'https://api.example.com/search')
            ->withHeader('Content-Type', 'application/json')
            ->withBody($factory->createStream('{"query":"x"}'));

        iterator_to_array(from_http_paginated(
            $client,
            $base,
            http_pagination_cursor('meta.next_cursor', http_request_option_body('cursor', $factory)),
        )->extract(flow_context(config())));

        $requests = $client->getRequests();
        static::assertSame(
            ['query' => 'x'],
            json_decode((string) $requests[0]->getBody(), true, 512, JSON_THROW_ON_ERROR),
        );
        static::assertSame(
            ['query' => 'x', 'cursor' => 'c2'],
            json_decode((string) $requests[1]->getBody(), true, 512, JSON_THROW_ON_ERROR),
        );
    }

    public function test_cursor_from_body_pagination(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c2'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c3'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => null], 'items' => [1]]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor')),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?cursor=c2',
                'https://api.example.com/items?cursor=c3',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_empty_first_page_stops_after_single_request(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['items' => []]));

        $rows = iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_page_number(http_request_option_query('page'), records_path: 'items'),
        )->extract(flow_context(config())));

        static::assertCount(1, $client->getRequests());
        static::assertCount(1, $rows);
    }

    public function test_inject_on_first_request_false(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['items' => []]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_page_number(
                http_request_option_query('page'),
                inject_on_first_request: false,
                records_path: 'items',
            ),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?page=2',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_invalid_json_throws_runtime_exception(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(new Response(200, ['Content-Type' => 'application/json'], 'not json {'));

        $this->expectException(RuntimeException::class);

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor')),
        )->extract(flow_context(config())));
    }

    public function test_last_record_cursor_pagination(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['data' => [['id' => 1], ['id' => 2]]]));
        $client->addResponse(PaginationMother::jsonResponse(['data' => [['id' => 3], ['id' => 4]]]));
        $client->addResponse(PaginationMother::jsonResponse(['data' => []]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_last_record_cursor('data.*.id', http_request_option_query('since')),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?since=2',
                'https://api.example.com/items?since=4',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_link_header_pagination(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['data' => [1]], [
            'Link' => '<https://api.example.com/items?page=2>; rel="next"',
        ]));
        $client->addResponse(PaginationMother::jsonResponse(['data' => [1]], [
            'Link' => '<https://api.example.com/items?page=3>; rel="next"',
        ]));
        $client->addResponse(PaginationMother::jsonResponse(['data' => [1]]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_link_header(),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?page=2',
                'https://api.example.com/items?page=3',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_max_pages_stop_composes_with_strategy_default(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c2'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c3'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c4'], 'items' => [1]]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_cursor(
                'meta.next_cursor',
                http_request_option_query('cursor'),
                stop_when: http_stop_when_max_pages(2),
            ),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?cursor=c2',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_next_url_from_body_pagination_with_escaped_path(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse([
            '@odata.nextLink' => 'https://api.example.com/items?page=2',
            'value' => [1],
        ]));
        $client->addResponse(PaginationMother::jsonResponse([
            '@odata.nextLink' => 'https://api.example.com/items?page=3',
            'value' => [1],
        ]));
        $client->addResponse(PaginationMother::jsonResponse(['value' => [1]]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_next_url('@odata\.nextLink'),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?page=2',
                'https://api.example.com/items?page=3',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_offset_limit_pagination_with_total(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['total' => 5], 'data' => [1, 2]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['total' => 5], 'data' => [3, 4]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['total' => 5], 'data' => [5]]));

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_offset(
                http_request_option_query('offset'),
                http_request_option_query('limit'),
                limit: 2,
                total_path: 'meta.total',
            ),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items?limit=2&offset=0',
                'https://api.example.com/items?limit=2&offset=2',
                'https://api.example.com/items?limit=2&offset=4',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
    }

    public function test_page_number_pagination(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['items' => [['id' => 1], ['id' => 2]]]));
        $client->addResponse(PaginationMother::jsonResponse(['items' => [['id' => 3], ['id' => 4]]]));
        $client->addResponse(PaginationMother::jsonResponse(['items' => []]));

        $rows = iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_page_number(http_request_option_query('page'), records_path: 'items'),
        )->extract(flow_context(config())));

        static::assertSame(
            [
                'https://api.example.com/items?page=1',
                'https://api.example.com/items?page=2',
                'https://api.example.com/items?page=3',
            ],
            array_map(
                static fn(RequestInterface $request): string => (string) $request->getUri(),
                $client->getRequests(),
            ),
        );
        static::assertCount(3, $rows);
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
            from_http_paginated(
                new Client(new Psr17Factory()),
                PaginationMother::request(),
                http_pagination_link_header(),
            )->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('response_body')),
            from_http_paginated(
                new Client(new Psr17Factory()),
                PaginationMother::request(),
                http_pagination_link_header(),
                schema(str_schema('response_body')),
            )->schema(),
        );
    }

    public function test_schema_typed_response_body(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['login' => 'flow-php', 'id' => 73_495_297]));

        $rows = iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/orgs/flow-php'),
            http_pagination_cursor('next', http_request_option_query('cursor')),
            schema(structure_schema('response_body', type_structure([
                'login' => type_string(),
                'id' => type_integer(),
            ]))),
        )->extract(flow_context(config())));

        static::assertEquals(
            type_structure(['login' => type_string(), 'id' => type_integer()]),
            $rows[0]->schema()->get('response_body')->type(),
        );
        static::assertSame(['login' => 'flow-php', 'id' => 73_495_297], $rows[0]->first()->get('response_body'));
    }

    public function test_schema_typed_response_body_with_missing_field(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['login' => 'flow-php']));

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('Rows do not match their schema: column "response_body" (row 0)');

        iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/orgs/flow-php'),
            http_pagination_cursor('next', http_request_option_query('cursor')),
            schema(structure_schema('response_body', type_structure([
                'login' => type_string(),
                'id' => type_integer(),
            ]))),
        )->extract(flow_context(config())));
    }

    public function test_schema_typed_response_body_via_with_schema(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['login' => 'flow-php', 'id' => 73_495_297]));

        $extractor = from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/orgs/flow-php'),
            http_pagination_cursor('next', http_request_option_query('cursor')),
        )->withSchema(schema(structure_schema('response_body', type_structure([
            'login' => type_string(),
            'id' => type_integer(),
        ]))));

        $rows = iterator_to_array($extractor->extract(flow_context(config())));

        static::assertEquals(
            type_structure(['login' => type_string(), 'id' => type_integer()]),
            $rows[0]->schema()->get('response_body')->type(),
        );
        static::assertSame(['login' => 'flow-php', 'id' => 73_495_297], $rows[0]->first()->get('response_body'));
    }

    public function test_stops_on_client_error_but_yields_its_row(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['items' => [1]]));
        $client->addResponse(new Response(500, [], 'Server Error'));

        $rows = iterator_to_array(from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_page_number(http_request_option_query('page'), records_path: 'items'),
        )->extract(flow_context(config())));

        static::assertCount(2, $client->getRequests());
        static::assertCount(2, $rows);
        static::assertSame(500, $rows[1]->first()->get('response_status_code'));
    }
}
