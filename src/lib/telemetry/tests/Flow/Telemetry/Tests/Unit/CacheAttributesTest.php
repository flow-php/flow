<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\CacheAttributes;
use PHPUnit\Framework\TestCase;

final class CacheAttributesTest extends TestCase
{
    public function test_cache_attribute_keys_follow_cache_proposal(): void
    {
        static::assertSame('cache.key', CacheAttributes::CACHE_KEY);
        static::assertSame('cache.key_count', CacheAttributes::CACHE_KEY_COUNT);
        static::assertSame('cache.operation', CacheAttributes::CACHE_OPERATION);
        static::assertSame('cache.pool', CacheAttributes::CACHE_POOL);
        static::assertSame('cache.prefix', CacheAttributes::CACHE_PREFIX);
        static::assertSame('cache.tags', CacheAttributes::CACHE_TAGS);
        static::assertSame('cache.tag_count', CacheAttributes::CACHE_TAG_COUNT);
        static::assertSame('cache.value_type', CacheAttributes::CACHE_VALUE_TYPE);
    }

    public function test_cache_span_names_are_low_cardinality(): void
    {
        static::assertSame('cache.clear', CacheAttributes::SPAN_CLEAR);
        static::assertSame('cache.delete', CacheAttributes::SPAN_DELETE);
        static::assertSame('cache.set', CacheAttributes::SPAN_SET);
    }
}
