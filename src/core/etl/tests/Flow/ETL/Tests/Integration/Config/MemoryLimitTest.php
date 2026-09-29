<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Config;

use Flow\ETL\Config\MemoryLimit;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function ini_set;
use function putenv;

final class MemoryLimitTest extends FlowIntegrationTestCase
{
    public function test_default_is_seventy_percent_of_the_memory_limit(): void
    {
        ini_set('memory_limit', '1G');

        static::assertSame(Unit::fromGb(1)->percentage(70)->inBytes(), MemoryLimit::default()->inBytes());
    }

    public function test_default_is_one_gib_when_the_memory_limit_is_unlimited(): void
    {
        ini_set('memory_limit', '-1');

        static::assertSame(Unit::fromGb(1)->inBytes(), MemoryLimit::default()->inBytes());
    }

    public function test_the_environment_wins_over_the_memory_limit(): void
    {
        ini_set('memory_limit', '1G');
        putenv(MemoryLimit::ENV . '=64M');

        try {
            static::assertSame(Unit::fromMb(64)->inBytes(), MemoryLimit::default()->inBytes());
        } finally {
            putenv(MemoryLimit::ENV);
        }
    }
}
