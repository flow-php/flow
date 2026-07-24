<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\Strategy\OffsetLimit;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class OffsetLimitTest extends FlowTestCase
{
    public function test_counts_limit_per_page(): void
    {
        static::assertSame(
            25,
            (new OffsetLimit(
                RequestOption::queryParam('offset'),
                RequestOption::queryParam('limit'),
                25,
            ))->countResults(PaginationMother::decoded(['data' => [1]])),
        );
    }

    public function test_initial_request_injects_limit_and_offset(): void
    {
        $strategy = new OffsetLimit(RequestOption::queryParam('offset'), RequestOption::queryParam('limit'), 25);

        static::assertSame(
            'limit=25&offset=0',
            $strategy->initialRequest(PaginationMother::request())->getUri()->getQuery(),
        );
    }

    public function test_next_request_advances_offset_by_limit(): void
    {
        $strategy = new OffsetLimit(RequestOption::queryParam('offset'), RequestOption::queryParam('limit'), 25);

        static::assertSame(
            'limit=25&offset=50',
            $strategy
                ->nextRequest(
                    PaginationMother::request(),
                    PaginationMother::decoded(['data' => [1]]),
                    new PageState(pagesFetched: 2),
                )
                ?->getUri()
                ->getQuery(),
        );
    }
}
