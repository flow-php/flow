<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Memory;

use Flow\ETL\Dataset\Memory\MemoryBudget;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\FlowTestCase;

use function memory_get_usage;

final class MemoryBudgetTest extends FlowTestCase
{
    public function test_a_process_under_the_limit_has_not_exceeded_it(): void
    {
        $budget = new MemoryBudget(
            new ReportedMemoryBackend(100_000_000),
            Unit::fromBytes(memory_get_usage(false) + 1_000_000_000),
        );

        static::assertFalse($budget->exceeded());
    }

    public function test_native_memory_counts_towards_the_limit(): void
    {
        $budget = new MemoryBudget(
            new ReportedMemoryBackend(2_000_000_000),
            Unit::fromBytes(memory_get_usage(false) + 1_000_000_000),
        );

        static::assertTrue($budget->exceeded());
    }

    public function test_the_limit_follows_the_memory_the_process_holds_now(): void
    {
        $backend = new ReportedMemoryBackend(2_000_000_000);
        $budget = new MemoryBudget($backend, Unit::fromBytes(memory_get_usage(false) + 1_000_000_000));

        static::assertTrue($budget->exceeded());

        $backend->reported = 0;
        $budget->released();

        static::assertFalse($budget->exceeded());
    }
}
