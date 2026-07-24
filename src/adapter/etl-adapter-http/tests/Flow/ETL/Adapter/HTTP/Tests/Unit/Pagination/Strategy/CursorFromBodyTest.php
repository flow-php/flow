<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\Strategy\CursorFromBody;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class CursorFromBodyTest extends FlowTestCase
{
    public function test_initial_request_is_unchanged(): void
    {
        $strategy = new CursorFromBody('meta.next_cursor', RequestOption::queryParam('cursor'));

        static::assertSame('', $strategy->initialRequest(PaginationMother::request())->getUri()->getQuery());
    }

    public function test_missing_cursor_stops(): void
    {
        $strategy = new CursorFromBody('meta.next_cursor', RequestOption::queryParam('cursor'));

        static::assertNull($strategy->nextRequest(
            PaginationMother::request(),
            PaginationMother::decoded(['meta' => []]),
            new PageState(pagesFetched: 1),
        ));
    }

    public function test_next_request_injects_cursor_from_body(): void
    {
        $strategy = new CursorFromBody('meta.next_cursor', RequestOption::queryParam('cursor'));

        static::assertSame(
            'cursor=abc123',
            $strategy
                ->nextRequest(
                    PaginationMother::request(),
                    PaginationMother::decoded(['meta' => ['next_cursor' => 'abc123']]),
                    new PageState(pagesFetched: 1),
                )
                ?->getUri()
                ->getQuery(),
        );
    }
}
