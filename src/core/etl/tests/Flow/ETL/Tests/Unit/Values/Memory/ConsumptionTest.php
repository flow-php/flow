<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Tests\Double\FixedProcessMemoryBackend;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Consumption;

final class ConsumptionTest extends FlowTestCase
{
    public function test_capturing_memory_consumption(): void
    {
        $consumption = new Consumption(new PhpBackend());

        $consumption->capture();

        static::assertGreaterThan(0, $consumption->max()->inBytes());
        static::assertGreaterThan(0, $consumption->min()->inBytes());
    }

    public function test_consumption_adds_the_backend_allocations(): void
    {
        $consumption = new Consumption(new FixedProcessMemoryBackend(1_048_576_000), realMemory: false);

        static::assertSame(1_048_576_000, $consumption->initial()->inBytes());
        static::assertSame(1_048_576_000, $consumption->capture()->inBytes());
    }

    public function test_normalize_gives_every_figure_in_bytes(): void
    {
        $consumption = new Consumption(new SpyBackend(1_048_576));

        static::assertSame(
            [
                'initial' => $consumption->initial()->inBytes(),
                'min' => $consumption->min()->inBytes(),
                'max' => $consumption->max()->inBytes(),
            ],
            $consumption->normalize(),
        );
    }
}
