<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\FlagFalse;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class FlagFalseTest extends FlowTestCase
{
    public function test_flag_false_stops(): void
    {
        static::assertTrue((new FlagFalse('has_more'))->shouldStop(PaginationMother::decoded([
            'has_more' => false,
        ]), new PageState()));
    }

    public function test_flag_true_does_not_stop(): void
    {
        static::assertFalse((new FlagFalse('has_more'))->shouldStop(PaginationMother::decoded([
            'has_more' => true,
        ]), new PageState()));
    }

    public function test_missing_flag_does_not_stop(): void
    {
        static::assertFalse((new FlagFalse('has_more'))->shouldStop(
            PaginationMother::decoded(['data' => []]),
            new PageState(),
        ));
    }
}
