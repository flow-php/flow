<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\Strategy\PageNumber;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class PageNumberTest extends FlowTestCase
{
    public function test_counts_results_at_records_path(): void
    {
        $strategy = new PageNumber(RequestOption::queryParam('page'), recordsPath: 'items');

        static::assertSame(3, $strategy->countResults(PaginationMother::decoded(['items' => [1, 2, 3]])));
    }

    public function test_initial_request_injects_page_and_size(): void
    {
        $strategy = new PageNumber(
            RequestOption::queryParam('page'),
            pageSize: 50,
            sizeOption: RequestOption::queryParam('per_page'),
        );

        static::assertSame(
            'per_page=50&page=1',
            $strategy->initialRequest(PaginationMother::request())->getUri()->getQuery(),
        );
    }

    public function test_initial_request_without_injection_on_first_request(): void
    {
        $strategy = new PageNumber(RequestOption::queryParam('page'), injectOnFirstRequest: false);

        static::assertSame('', $strategy->initialRequest(PaginationMother::request())->getUri()->getQuery());
    }

    public function test_next_request_increments_page_from_start(): void
    {
        $strategy = new PageNumber(RequestOption::queryParam('page'), startPage: 1);

        static::assertSame(
            'page=2',
            $strategy
                ->nextRequest(
                    PaginationMother::request(),
                    PaginationMother::decoded(['items' => [1]]),
                    new PageState(pagesFetched: 1),
                )
                ?->getUri()
                ->getQuery(),
        );
    }
}
