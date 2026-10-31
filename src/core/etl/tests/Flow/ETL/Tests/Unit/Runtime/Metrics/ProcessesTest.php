<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics;

use Flow\ETL\Runtime\Metrics\Processes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\Unit;

final class ProcessesTest extends FlowTestCase
{
    public function test_a_serial_run_has_no_worker_peak(): void
    {
        $processes = new Processes(Unit::fromBytes(25), []);

        static::assertNull($processes->workerPeak());
        static::assertEquals(Unit::fromBytes(25), $processes->peakSum());
    }

    public function test_the_worker_peak_is_the_largest_worker(): void
    {
        static::assertEquals(Unit::fromBytes(41), (new Processes(Unit::fromBytes(25), [
            Unit::fromBytes(40),
            Unit::fromBytes(41),
            Unit::fromBytes(39),
        ]))->workerPeak());
    }

    public function test_the_peak_sum_adds_the_parent_and_every_worker(): void
    {
        static::assertEquals(Unit::fromBytes(145), (new Processes(Unit::fromBytes(25), [
            Unit::fromBytes(40),
            Unit::fromBytes(41),
            Unit::fromBytes(39),
        ]))->peakSum());
    }

    public function test_normalize_gives_every_peak_in_bytes(): void
    {
        static::assertSame(
            ['parent' => 25, 'workers' => [40, 41], 'worker_peak' => 41, 'peak_sum' => 106],
            (new Processes(Unit::fromBytes(25), [Unit::fromBytes(40), Unit::fromBytes(41)]))->normalize(),
        );
        static::assertSame(
            ['parent' => 25, 'workers' => [], 'worker_peak' => null, 'peak_sum' => 25],
            (new Processes(Unit::fromBytes(25), []))->normalize(),
        );
    }
}
