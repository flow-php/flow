<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Row;

final class AllTest extends TestCase
{
    public function test_failure() : void
    {
        $comparison1 = $this->createStub(Comparison::class);
        $comparison1
            ->method('compare')
            ->willReturn(true);

        $comparison2 = $this->createStub(Comparison::class);
        $comparison2
            ->method('compare')
            ->willReturn(false);

        $this->assertFalse(
            (new All($comparison1, $comparison2))
                ->compare(
                    Row::create(int_entry('id', 1)),
                    Row::create(int_entry('id', 2)),
                )
        );
    }

    public function test_success() : void
    {
        $comparison1 = $this->createStub(Comparison::class);
        $comparison1
            ->method('compare')
            ->willReturn(true);

        $comparison2 = $this->createStub(Comparison::class);
        $comparison2
            ->method('compare')
            ->willReturn(true);

        $this->assertTrue(
            (new All($comparison1, $comparison2))
                ->compare(
                    Row::create(int_entry('id', 1)),
                    Row::create(int_entry('id', 2)),
                )
        );
    }
}
