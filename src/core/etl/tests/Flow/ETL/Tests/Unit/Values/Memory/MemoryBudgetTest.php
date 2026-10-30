<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;

use function memory_get_usage;

final class MemoryBudgetTest extends FlowTestCase
{
    public function test_a_process_under_the_limit_has_not_exceeded_it(): void
    {
        static::assertFalse(
            (new MemoryBudget(
                new ReportedMemoryBackend(100_000_000),
                Unit::fromBytes(memory_get_usage(false) + 1_000_000_000),
            ))->exceeded(),
        );
    }

    public function test_native_memory_counts_towards_the_limit(): void
    {
        static::assertTrue(
            (new MemoryBudget(
                new ReportedMemoryBackend(2_000_000_000),
                Unit::fromBytes(memory_get_usage(false) + 1_000_000_000),
            ))->exceeded(),
        );
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
