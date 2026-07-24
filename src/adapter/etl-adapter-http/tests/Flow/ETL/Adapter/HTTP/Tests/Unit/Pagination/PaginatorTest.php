<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\Paginator;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\NeverStop;
use Flow\ETL\Adapter\HTTP\Tests\Double\FixedStrategy;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Factory\Psr17Factory;

final class PaginatorTest extends FlowTestCase
{
    public function test_continues_on_client_error_when_disabled(): void
    {
        $base = PaginationMother::request('GET', 'https://api.example.com/items');
        $next = PaginationMother::request('GET', 'https://api.example.com/items?page=2');
        $paginator = new Paginator(new FixedStrategy($next), new NeverStop(), stopOnClientError: false);

        $paginator->initialRequest($base);
        $next = $paginator->nextRequest($base, new DecodedResponse([], (new Psr17Factory())->createResponse(500)));

        static::assertNotNull($next);
        static::assertSame('https://api.example.com/items?page=2', (string) $next->getUri());
    }

    public function test_loop_guard_stops_on_identical_request(): void
    {
        $base = PaginationMother::request('GET', 'https://api.example.com/items');
        $paginator = new Paginator(new FixedStrategy($base), new NeverStop());

        $paginator->initialRequest($base);

        static::assertNull($paginator->nextRequest($base, PaginationMother::decoded(['data' => [1]])));
    }

    public function test_stops_on_client_error_by_default(): void
    {
        $base = PaginationMother::request('GET', 'https://api.example.com/items');
        $next = PaginationMother::request('GET', 'https://api.example.com/items?page=2');
        $paginator = new Paginator(new FixedStrategy($next), new NeverStop());

        $paginator->initialRequest($base);

        static::assertNull($paginator->nextRequest(
            $base,
            new DecodedResponse([], (new Psr17Factory())->createResponse(500)),
        ));
    }
}
