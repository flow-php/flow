<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\Strategy;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\LastRecordCursor;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
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
