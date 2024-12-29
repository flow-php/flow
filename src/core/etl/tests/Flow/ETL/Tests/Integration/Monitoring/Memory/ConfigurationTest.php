<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Monitoring\Memory;

use Flow\ETL\Monitoring\Memory\{Configuration, Unit};
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

final class ConfigurationTest extends FlowIntegrationTestCase
{
    public function test_less_than_for_infinite_memory() : void
    {
        \ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        self::assertFalse($config->isLessThan(Unit::fromGb(1_000_000)));
    }

    public function test_less_than_for_set_memory() : void
    {
        \ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        self::assertTrue($config->isLessThan(Unit::fromGb(1_000_000)));
        self::assertFalse($config->isLessThan(Unit::fromGb(1)));
        self::assertFalse($config->isLessThan(Unit::fromMb(100)));
    }

    public function test_memory_limit_fixed() : void
    {
        \ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        self::assertEquals(Unit::fromGb(1), $config->limit());
    }

    public function test_memory_limit_fixed_with_safety_buffer() : void
    {
        \ini_set('memory_limit', '1G');

        $config = new Configuration(10);

        self::assertEquals(Unit::fromMb(900), $config->limit());
    }

    public function test_memory_limit_infinite() : void
    {
        \ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        self::assertNull($config->limit());
    }

    public function test_memory_limit_wift_safety_buffer() : void
    {
        \ini_set('memory_limit', '1G');

        $config = new Configuration(10);

        self::assertEquals(
            Unit::fromString('900M'),
            $config->limit()
        );
    }

    public function test_unit_below_limit_percentage_for_fixed_memory() : void
    {
        \ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        self::assertTrue($config->isConsumptionBelow(Unit::fromMb(99), 10));
        self::assertFalse($config->isConsumptionBelow(Unit::fromMb(100), 10));
    }

    public function test_unit_below_limit_percentage_for_infinite_memory() : void
    {
        \ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        self::assertTrue($config->isConsumptionBelow(Unit::fromGb(1_000_000), 10));
    }
}
