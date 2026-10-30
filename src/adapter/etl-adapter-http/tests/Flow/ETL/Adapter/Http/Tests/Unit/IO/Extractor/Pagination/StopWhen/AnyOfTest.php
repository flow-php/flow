<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Http\http_stop_when_any_of;
use function Flow\ETL\Adapter\Http\http_stop_when_max_pages;
use function Flow\ETL\Adapter\Http\http_stop_when_path_missing;

final class AnyOfTest extends FlowTestCase
{
    public function test_stops_when_any_condition_stops(): void
    {
        $condition = http_stop_when_any_of(http_stop_when_path_missing('meta.next'), http_stop_when_max_pages(3));

        static::assertTrue($condition->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => 'abc',
        ]]), new PageState(pagesFetched: 3)));
    }

    public function test_does_not_stop_when_no_condition_stops(): void
    {
        $condition = http_stop_when_any_of(http_stop_when_path_missing('meta.next'), http_stop_when_max_pages(3));

        static::assertFalse($condition->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => 'abc',
        ]]), new PageState(pagesFetched: 1)));
    }
}
