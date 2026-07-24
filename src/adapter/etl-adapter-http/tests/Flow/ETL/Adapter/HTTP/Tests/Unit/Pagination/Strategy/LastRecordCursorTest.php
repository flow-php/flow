<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\Strategy\LastRecordCursor;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class LastRecordCursorTest extends FlowTestCase
{
    public function test_counts_records(): void
    {
        $strategy = new LastRecordCursor('data.*.id', RequestOption::queryParam('since'));

        static::assertSame(
            3,
            $strategy->countResults(PaginationMother::decoded([
                'data' => [['id' => 1], ['id' => 2], ['id' => 3]],
            ])),
        );
    }

    public function test_empty_records_stops(): void
    {
        $strategy = new LastRecordCursor('data.*.id', RequestOption::queryParam('since'));

        static::assertNull($strategy->nextRequest(
            PaginationMother::request(),
            PaginationMother::decoded(['data' => []]),
            new PageState(pagesFetched: 1),
        ));
    }

    public function test_next_request_injects_last_record_value(): void
    {
        $strategy = new LastRecordCursor('data.*.id', RequestOption::queryParam('since'));

        static::assertSame(
            'since=42',
            $strategy
                ->nextRequest(
                    PaginationMother::request(),
                    PaginationMother::decoded(['data' => [['id' => 40], ['id' => 41], ['id' => 42]]]),
                    new PageState(pagesFetched: 1),
                )
                ?->getUri()
                ->getQuery(),
        );
    }
}
