<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Memory;

use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Tests\FlowTestCase;

final class ConsumptionTest extends FlowTestCase
{
    public function test_capturing_memory_consumption() : void
    {
        $consumption = new Consumption();

        $consumption->capture();

        self::assertGreaterThan(0, $consumption->max()->inBytes());
        self::assertGreaterThan(0, $consumption->min()->inBytes());
    }
}
