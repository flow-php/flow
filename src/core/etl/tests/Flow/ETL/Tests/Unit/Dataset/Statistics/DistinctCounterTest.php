<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use Flow\ETL\Dataset\Statistics\DistinctCounter;
use Flow\ETL\Tests\FlowTestCase;

final class DistinctCounterTest extends FlowTestCase
{
    public function test_distinc_counter_on_date_time() : void
    {
        $counter = new DistinctCounter();

        $counter->add(new \DateTimeImmutable('2023-01-01 00:00:01 UTC'));
        $counter->add(new \DateTimeImmutable('2023-01-02 00:00:01 UTC'));
        $counter->add(new \DateTimeImmutable('2023-01-01 00:00:01 UTC')); // duplicate
        $counter->add(new \DateTimeImmutable('2023-01-02 00:00:01 Europe/Warsaw'));
        $counter->add(new \DateTimeImmutable('2023-01-01 00:00:01+00:00')); // duplicate

        self::assertEquals(3, $counter->count());
    }

    public function test_distinct_counter() : void
    {
        $counter = new DistinctCounter();

        $counter->add('a');
        $counter->add('b');
        $counter->add('c');
        $counter->add('a'); // Duplicate
        $counter->add(1);
        $counter->add(2);
        $counter->add(1); // Duplicate
        $counter->add(1.5);

        self::assertEquals(6, $counter->count());
    }
}
