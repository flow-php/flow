<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime;

use Flow\ETL\Runtime\CacheConfig;
use Flow\ETL\Tests\FlowTestCase;
use ReflectionClass;

use function array_map;

final class CacheConfigTest extends FlowTestCase
{
    public function test_cache_config_no_longer_carries_a_filesystem_mount(): void
    {
        $constructor = (new ReflectionClass(CacheConfig::class))->getConstructor();

        static::assertNotNull($constructor);
        static::assertSame(
            ['cache', 'localFilesystemCacheDir'],
            array_map(static fn($parameter): string => $parameter->getName(), $constructor->getParameters()),
        );
    }
}
