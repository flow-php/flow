<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;

final class AllTest extends FlowTestCase
{
    public function test_failure(): void
    {
        $comparison1 = self::createStub(Comparison::class);
        $comparison1->method('compare')->willReturn(true);

        $comparison2 = self::createStub(Comparison::class);
        $comparison2->method('compare')->willReturn(false);

        static::assertFalse((new All($comparison1, $comparison2))->compare(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        ));
    }

    public function test_success(): void
    {
        $comparison1 = self::createStub(Comparison::class);
        $comparison1->method('compare')->willReturn(true);

        $comparison2 = self::createStub(Comparison::class);
        $comparison2->method('compare')->willReturn(true);

        static::assertTrue((new All($comparison1, $comparison2))->compare(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        ));
    }
}
