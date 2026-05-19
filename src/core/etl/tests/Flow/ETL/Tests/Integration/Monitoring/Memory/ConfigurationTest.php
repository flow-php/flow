<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Monitoring\Memory;

use Flow\ETL\Dataset\Memory\Configuration;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function ini_set;

final class ConfigurationTest extends FlowIntegrationTestCase
{
    public function test_less_than_for_infinite_memory(): void
    {
        ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        static::assertFalse($config->isLessThan(Unit::fromGb(1_000_000)));
    }

    public function test_less_than_for_set_memory(): void
    {
        ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        static::assertTrue($config->isLessThan(Unit::fromGb(1_000_000)));
        static::assertFalse($config->isLessThan(Unit::fromGb(1)));
        static::assertFalse($config->isLessThan(Unit::fromMb(100)));
    }

    public function test_memory_limit_fixed(): void
    {
        ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        static::assertEquals(Unit::fromGb(1), $config->limit());
    }

    public function test_memory_limit_fixed_with_safety_buffer(): void
    {
        ini_set('memory_limit', '1G');

        $config = new Configuration(10);

        static::assertEquals(Unit::fromMb(900), $config->limit());
    }

    public function test_memory_limit_infinite(): void
    {
        ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        static::assertNull($config->limit());
    }

    public function test_memory_limit_wift_safety_buffer(): void
    {
        ini_set('memory_limit', '1G');

        $config = new Configuration(10);

        static::assertEquals(Unit::fromString('900M'), $config->limit());
    }

    public function test_unit_below_limit_percentage_for_fixed_memory(): void
    {
        ini_set('memory_limit', '1G');

        $config = new Configuration(0);

        static::assertTrue($config->isConsumptionBelow(Unit::fromMb(99), 10));
        static::assertFalse($config->isConsumptionBelow(Unit::fromMb(100), 10));
    }

    public function test_unit_below_limit_percentage_for_infinite_memory(): void
    {
        ini_set('memory_limit', '-1');

        $config = new Configuration(0);

        static::assertTrue($config->isConsumptionBelow(Unit::fromGb(1_000_000), 10));
    }
}
