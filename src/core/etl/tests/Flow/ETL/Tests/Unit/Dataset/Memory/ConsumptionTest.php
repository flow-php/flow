<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Memory;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;

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
        $php = new Consumption(new PhpBackend());
        $native = new Consumption(new SpyBackend(1_048_576));

        static::assertSame(1_048_576, $native->initial()->inBytes() - $php->initial()->inBytes());
        static::assertSame(1_048_576, $native->capture()->inBytes() - $php->capture()->inBytes());
    }
}
