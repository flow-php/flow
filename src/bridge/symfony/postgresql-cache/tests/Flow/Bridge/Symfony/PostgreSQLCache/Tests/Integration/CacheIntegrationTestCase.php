<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Integration;

use PHPUnit\Framework\TestCase;

abstract class CacheIntegrationTestCase extends TestCase
{
    private ?CacheTestContext $context = null;

    protected function setUp(): void
    {
        if (!\extension_loaded('pgsql')) {
            static::markTestSkipped('ext-pgsql is not available');
        }

        $this->context = new CacheTestContext();
        $this->context->dropCacheTable('cache_items');
        $this->context->createCacheTable('cache_items');
    }

    protected function tearDown(): void
    {
        if ($this->context !== null) {
            $this->context->dropCacheTable('cache_items');
            $this->context->close();
            $this->context = null;
        }
    }

    protected function cacheContext(): CacheTestContext
    {
        if ($this->context === null) {
            static::fail('CacheTestContext not initialized. Ensure setUp() was called.');
        }

        return $this->context;
    }
}
