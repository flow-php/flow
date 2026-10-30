<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\FlagTrue;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class FlagTrueTest extends FlowTestCase
{
    public function test_flag_false_does_not_stop(): void
    {
        static::assertFalse((new FlagTrue('is_last'))->shouldStop(PaginationMother::decoded([
            'is_last' => false,
        ]), new PageState()));
    }

    public function test_flag_true_stops(): void
    {
        static::assertTrue((new FlagTrue('is_last'))->shouldStop(PaginationMother::decoded([
            'is_last' => true,
        ]), new PageState()));
    }

    public function test_missing_flag_does_not_stop(): void
    {
        static::assertFalse((new FlagTrue('is_last'))->shouldStop(
            PaginationMother::decoded(['data' => []]),
            new PageState(),
        ));
    }
}
