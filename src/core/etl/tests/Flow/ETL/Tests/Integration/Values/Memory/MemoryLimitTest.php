<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Values\Memory;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Memory\MemoryLimit;
use Flow\ETL\Values\Memory\Unit;

use function ini_get;
use function ini_set;
use function putenv;

final class MemoryLimitTest extends FlowIntegrationTestCase
{
    public function test_default_is_seventy_percent_of_the_memory_limit(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');

        try {
            static::assertSame(Unit::fromGb(1)->percentage(70)->inBytes(), MemoryLimit::default()->inBytes());
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_default_is_one_gib_when_the_memory_limit_is_unlimited(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');

        try {
            static::assertSame(Unit::fromGb(1)->inBytes(), MemoryLimit::default()->inBytes());
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_the_environment_is_the_default_when_the_memory_limit_is_unlimited(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');
        putenv(MemoryLimit::ENV . '=64M');

        try {
            static::assertSame(Unit::fromMb(64)->inBytes(), MemoryLimit::default()->inBytes());
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_default_refuses_an_environment_above_the_memory_limit(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');
        putenv(MemoryLimit::ENV . '=2G');

        try {
            $this->expectExceptionObject(new InvalidArgumentException(
                'FLOW_MAX_MEMORY=2G is above memory_limit=1G, so the process cap could never be reached; '
                . 'lower FLOW_MAX_MEMORY or raise memory_limit.',
            ));

            MemoryLimit::default();
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_the_environment_wins_over_the_memory_limit(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');
        putenv(MemoryLimit::ENV . '=64M');

        try {
            static::assertSame(Unit::fromMb(64)->inBytes(), MemoryLimit::default()->inBytes());
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_process_cap_is_seventy_percent_of_the_memory_limit(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');

        try {
            static::assertSame(
                Unit::fromString('1G')->percentage(70)->inBytes(),
                MemoryLimit::processCap()?->inBytes(),
            );
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_process_cap_is_null_when_the_memory_limit_is_unlimited(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');

        try {
            static::assertNull(MemoryLimit::processCap());

            putenv(MemoryLimit::ENV . '=64M');

            static::assertNull(MemoryLimit::processCap());
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_the_environment_sets_the_process_cap(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');
        putenv(MemoryLimit::ENV . '=64M');

        try {
            static::assertSame(Unit::fromMb(64)->inBytes(), MemoryLimit::processCap()?->inBytes());
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_an_environment_above_the_memory_limit_is_refused(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '1G');
        putenv(MemoryLimit::ENV . '=2G');

        try {
            $this->expectExceptionObject(new InvalidArgumentException(
                'FLOW_MAX_MEMORY=2G is above memory_limit=1G, so the process cap could never be reached; '
                . 'lower FLOW_MAX_MEMORY or raise memory_limit.',
            ));

            MemoryLimit::processCap();
        } finally {
            putenv(MemoryLimit::ENV);
            ini_set('memory_limit', $memoryLimit);
        }
    }
}
