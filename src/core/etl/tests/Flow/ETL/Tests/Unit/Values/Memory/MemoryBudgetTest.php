<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Tests\Double\FixedProcessMemoryBackend;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;
use PHPUnit\Framework\Attributes\TestWith;

use function intdiv;
use function memory_get_usage;
use function str_repeat;

use const PHP_INT_MAX;

final class MemoryBudgetTest extends FlowTestCase
{
    public function test_memory_held_before_the_budget_starts_does_not_count(): void
    {
        static::assertFalse(
            (new MemoryBudget(new ReportedMemoryBackend(2_000_000_000), Unit::fromGb(1), null))->exceeded(),
        );
    }

    public function test_growth_past_the_limit_exceeds_it(): void
    {
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromGb(1), null);
        $backend->reported = 1_100_000_000;

        static::assertTrue($budget->exceeded());
    }

    public function test_exceeded_with_multiplies_the_growth(): void
    {
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromGb(1), null);
        $backend->reported = 600_000_000;

        static::assertFalse($budget->exceeded());
        static::assertTrue($budget->exceededWith(2));
    }

    public function test_released_measures_growth_from_the_release(): void
    {
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromGb(1), null);
        $backend->reported = 900_000_000;
        $budget->released();
        $backend->reported = 940_000_000;

        static::assertFalse($budget->exceededWith(2));
    }

    public function test_released_keeps_cached_memory_out_of_the_measure(): void
    {
        $budget = new MemoryBudget(new ReportedMemoryBackend(0), Unit::fromMb(1), null);
        $allocated = str_repeat('x', 10_000_000);
        unset($allocated);
        $budget->released();

        static::assertFalse($budget->exceeded());
    }

    public function test_over_the_cap_spills_at_the_floor(): void
    {
        $backend = new ReportedMemoryBackend(Unit::fromGb(2)->inBytes());
        $budget = new MemoryBudget($backend, Unit::fromGb(1), Unit::fromGb(1));
        $backend->reported += Unit::fromMb(65)->inBytes();

        static::assertTrue($budget->exceeded());
    }

    public function test_over_the_cap_below_the_floor_does_not_spill(): void
    {
        $backend = new ReportedMemoryBackend(Unit::fromGb(2)->inBytes());
        $budget = new MemoryBudget($backend, Unit::fromGb(1), Unit::fromGb(1));
        $backend->reported += Unit::fromMb(1)->inBytes();

        static::assertFalse($budget->exceeded());
    }

    public function test_exceeded_with_projects_the_copy_onto_the_cap(): void
    {
        $cap = Unit::fromGb(1);
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromGb(10), $cap);
        // process + growth stays under the cap, process + twice the growth passes it
        $backend->reported = intdiv(3 * ($cap->inBytes() - memory_get_usage(false)), 4);

        static::assertFalse($budget->exceeded());
        static::assertTrue($budget->exceededWith(2));
    }

    public function test_under_the_cap_only_the_limit_counts(): void
    {
        $backend = new ReportedMemoryBackend(0);
        $budget = new MemoryBudget($backend, Unit::fromGb(1), Unit::fromGb(100));
        $backend->reported = Unit::fromMb(500)->inBytes();

        static::assertFalse($budget->exceeded());
    }

    public function test_without_a_cap_process_memory_never_counts(): void
    {
        $backend = new ReportedMemoryBackend(100_000_000_000);
        $budget = new MemoryBudget($backend, Unit::fromGb(1), null);
        $backend->reported += 1_000_000;

        static::assertFalse($budget->exceeded());
    }

    public function test_an_allocation_past_the_cap_crosses_it(): void
    {
        $cap = Unit::fromGb(2);
        // the process sits 10 MB under the cap
        $budget = new MemoryBudget(
            new ReportedMemoryBackend($cap->inBytes() - memory_get_usage(false) - Unit::fromMb(10)->inBytes()),
            Unit::fromGb(1),
            $cap,
        );

        static::assertFalse($budget->crossesCapWith(Unit::fromMb(1)->inBytes()));
        static::assertTrue($budget->crossesCapWith(Unit::fromMb(20)->inBytes()));
    }

    public function test_an_allocation_up_to_exactly_the_cap_does_not_cross_it(): void
    {
        $budget = new MemoryBudget(
            new FixedProcessMemoryBackend(Unit::fromGb(1)->inBytes()),
            Unit::fromGb(1),
            Unit::fromGb(2),
        );

        static::assertFalse($budget->crossesCapWith(Unit::fromGb(1)->inBytes()));
        static::assertTrue($budget->crossesCapWith(Unit::fromGb(1)->inBytes() + 1));
    }

    public function test_without_a_cap_no_allocation_crosses_it(): void
    {
        static::assertFalse((new MemoryBudget(
            new ReportedMemoryBackend(0),
            Unit::fromGb(1),
            null,
        ))->crossesCapWith(PHP_INT_MAX >> 1));
    }

    public function test_remaining_is_the_limit_minus_own_growth(): void
    {
        $backend = new ReportedMemoryBackend(2_000_000_000);
        $budget = new MemoryBudget($backend, Unit::fromBytes(1_000_000_000), null);
        $backend->reported += 400_000_000;

        static::assertEqualsWithDelta(600_000_000, $budget->remaining(), 1_000_000);
    }

    public function test_remaining_over_the_cap_is_the_floor_minus_own_growth(): void
    {
        $backend = new ReportedMemoryBackend(Unit::fromGb(2)->inBytes());
        $budget = new MemoryBudget($backend, Unit::fromGb(1), Unit::fromGb(1));
        $backend->reported += Unit::fromMb(10)->inBytes();

        static::assertEqualsWithDelta(Unit::fromMb(54)->inBytes(), $budget->remaining(), 1_000_000);
    }

    #[TestWith([1, 64])]
    #[TestWith([4, 128])]
    public function test_floor_is_a_sixteenth_of_the_cap_up_to_128_mib(int $capGb, int $floorMb): void
    {
        static::assertSame(
            Unit::fromMb($floorMb)->inBytes(),
            (new MemoryBudget(new ReportedMemoryBackend(0), Unit::fromGb(1), Unit::fromGb($capGb)))->floor(),
        );
    }

    public function test_floor_without_a_cap_is_the_limit(): void
    {
        static::assertSame(
            Unit::fromMb(300)->inBytes(),
            (new MemoryBudget(new ReportedMemoryBackend(0), Unit::fromMb(300), null))->floor(),
        );
    }
}
