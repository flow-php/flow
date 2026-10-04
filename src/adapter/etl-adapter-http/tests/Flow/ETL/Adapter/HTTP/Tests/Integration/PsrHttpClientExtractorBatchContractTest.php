<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use Flow\ETL\Adapter\HTTP\Tests\Double\NumberedRequestFactory;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;

use function array_map;
use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\Adapter\Http\from_http_paginated;
use function Flow\ETL\Adapter\Http\from_static_http_requests;
use function Flow\ETL\Adapter\Http\http_pagination_cursor;
use function Flow\ETL\Adapter\Http\http_request_option_query;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function iterator_to_array;

/**
 * One exchange is one row. The dynamic and paginated extractors yield one row per batch; the static one does too
 * unless withBatchSize() groups exchanges, and it never sends more requests than the limit asks for.
 */
final class PsrHttpClientExtractorBatchContractTest extends FlowTestCase
{
    public function test_a_stopped_dynamic_extractor_sends_no_further_request(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(new Response(200, [], '{"id":1}'));
        $client->addResponse(new Response(200, [], '{"id":2}'));

        $generator = from_dynamic_http_requests(
            $client,
            new NumberedRequestFactory('https://api.example.com/', 2),
        )->extract(flow_context(config()));
        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertCount(1, $client->getRequests());
    }

    public function test_a_stopped_paginated_extractor_requests_no_further_page(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c2'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => null], 'items' => [1]]));

        $generator = from_http_paginated(
            $client,
            PaginationMother::request('GET', 'https://api.example.com/items'),
            http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor')),
        )->extract(flow_context(config()));
        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertCount(1, $client->getRequests());
    }

    public function test_a_stopped_static_extractor_sends_no_further_request(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(new Response(200, [], '{"id":1}'));
        $client->addResponse(new Response(200, [], '{"id":2}'));

        $generator = from_static_http_requests($client, [
            $factory->createRequest('GET', 'https://api.example.com/1'),
            $factory->createRequest('GET', 'https://api.example.com/2'),
        ])->extract(flow_context(config()));
        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertCount(1, $client->getRequests());
    }

    public function test_dynamic_extractor_yields_one_row_per_batch_and_every_exchange(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(new Response(200, [], '{"id":1}'));
        $client->addResponse(new Response(200, [], '{"id":2}'));

        $batches = iterator_to_array(
            from_dynamic_http_requests($client, new NumberedRequestFactory('https://api.example.com/', 2))->extract(
                flow_context(config()),
            ),
            false,
        );

        static::assertSame([1, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame(
            ['https://api.example.com/1', 'https://api.example.com/2'],
            array_map(static fn(Rows $rows): mixed => $rows->column('request_uri')->value(0), $batches),
        );
    }

    public function test_paginated_extractor_yields_one_row_per_page_and_every_page(): void
    {
        $client = new Client(new Psr17Factory());
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c2'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => 'c3'], 'items' => [1]]));
        $client->addResponse(PaginationMother::jsonResponse(['meta' => ['next_cursor' => null], 'items' => [1]]));

        $batches = iterator_to_array(
            from_http_paginated(
                $client,
                PaginationMother::request('GET', 'https://api.example.com/items'),
                http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor')),
            )->extract(flow_context(config())),
            false,
        );

        static::assertSame([1, 1, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame(
            [
                'https://api.example.com/items',
                'https://api.example.com/items?cursor=c2',
                'https://api.example.com/items?cursor=c3',
            ],
            array_map(static fn(Rows $rows): mixed => $rows->column('request_uri')->value(0), $batches),
        );
    }

    public function test_static_extractor_yields_one_row_per_batch_and_every_exchange(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);
        $client->addResponse(new Response(200, [], '{"id":1}'));
        $client->addResponse(new Response(200, [], '{"id":2}'));

        $batches = iterator_to_array(
            from_static_http_requests($client, [
                $factory->createRequest('GET', 'https://api.example.com/1'),
                $factory->createRequest('GET', 'https://api.example.com/2'),
            ])->extract(flow_context(config())),
            false,
        );

        static::assertSame([1, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame(
            ['https://api.example.com/1', 'https://api.example.com/2'],
            array_map(static fn(Rows $rows): mixed => $rows->column('request_uri')->value(0), $batches),
        );
    }

    public function test_a_static_extractor_groups_exchanges_into_batches_of_its_batch_size(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);

        foreach ([1, 2, 3] as $id) {
            $client->addResponse(new Response(200, [], '{"id":' . $id . '}'));
        }

        $batches = iterator_to_array(
            from_static_http_requests($client, array_map(static fn(int $id) => $factory->createRequest(
                'GET',
                'https://api.example.com/' . $id,
            ), [1, 2, 3]))
                ->withBatchSize(2)
                ->extract(flow_context(config())),
            false,
        );

        static::assertSame([2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
    }

    public function test_a_static_extractor_sends_no_more_requests_than_the_limit_and_caps_the_batch(): void
    {
        $factory = new Psr17Factory();
        $client = new Client($factory);

        foreach ([1, 2, 3] as $id) {
            $client->addResponse(new Response(200, [], '{"id":' . $id . '}'));
        }

        $batches = iterator_to_array(
            from_static_http_requests($client, array_map(static fn(int $id) => $factory->createRequest(
                'GET',
                'https://api.example.com/' . $id,
            ), [1, 2, 3]))
                ->withBatchSize(5)
                ->extract(flow_context(config()), 2),
            false,
        );

        static::assertSame([2], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertCount(2, $client->getRequests());
    }
}
