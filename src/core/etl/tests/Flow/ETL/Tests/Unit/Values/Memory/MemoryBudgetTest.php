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

    public function test_exceeded_with_multiplies_only_the_memory_grown_since_the_budget_was_made(): void
    {
        $backend = new ReportedMemoryBackend(1_000_000_000);
        $budget = new MemoryBudget($backend, Unit::fromBytes(memory_get_usage(false) + 1_700_000_000));
        $backend->reported = 1_400_000_000;

        static::assertFalse($budget->exceeded());
        static::assertTrue($budget->exceededWith(2));
    }

    public function test_exceeded_with_counts_the_growth_from_the_last_release(): void
    {
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromBytes(memory_get_usage(false) + 1_000_000_000));
        $backend->reported = 900_000_000;
        $budget->released();
        $backend->reported = 940_000_000;

        static::assertFalse($budget->exceededWith(2));
    }
}
