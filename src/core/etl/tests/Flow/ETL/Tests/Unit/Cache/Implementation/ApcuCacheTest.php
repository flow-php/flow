<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache\Implementation;

use Flow\ETL\Cache\Implementation\ApcuCache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Tests\FlowTestCase;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function apcu_enabled;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;

#[CoversClass(ApcuCache::class)]
#[RequiresPhpExtension('apcu')]
final class ApcuCacheTest extends FlowTestCase
{
    private ApcuCache $cache;

    private ApcuCache $otherCache;

    #[Override]
    protected function setUp(): void
    {
        if (!apcu_enabled()) {
            static::markTestSkipped('APCu is not enabled for CLI (apc.enable_cli=0).');
        }

        $this->cache = new ApcuCache('flow_php_cache_test');
        $this->otherCache = new ApcuCache('flow_php_cache_test_other');

        $this->cache->clear();
        $this->otherCache->clear();
    }

    #[Override]
    protected function tearDown(): void
    {
        if (!isset($this->cache)) {
            return;
        }

        $this->cache->clear();
        $this->otherCache->clear();
    }

    public function test_caching_rows(): void
    {
        static::assertFalse($this->cache->has('rows'));

        $this->cache->set(
            'rows',
            $rows = rows(
                row(int_entry('id', 1), str_entry('name', 'John')),
                row(int_entry('id', 2), str_entry('name', 'Jane')),
            ),
        );

        static::assertTrue($this->cache->has('rows'));
        static::assertEquals($rows, $this->cache->get('rows'));
    }

    public function test_checking_on_non_existing_cache_key(): void
    {
        static::assertFalse($this->cache->has('non-existing'));
    }

    public function test_clearing_cache_removes_only_its_own_namespace(): void
    {
        $this->cache->set('rows', rows(row(int_entry('id', 1))));
        $this->otherCache->set('rows', rows(row(int_entry('id', 2))));

        $this->cache->clear();

        static::assertFalse($this->cache->has('rows'));
        static::assertTrue($this->otherCache->has('rows'));
    }

    public function test_getting_non_existing_cache_key(): void
    {
        $this->expectException(KeyNotInCacheException::class);

        $this->cache->get('non-existing');
    }

    public function test_reading_rows(): void
    {
        $this->cache->set('rows', $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2))));

        static::assertEquals([$rows], iterator_to_array($this->cache->read('rows'), false));
    }

    public function test_reading_non_existing_cache_key(): void
    {
        $this->expectException(KeyNotInCacheException::class);

        iterator_to_array($this->cache->read('non-existing'), false);
    }

    public function test_removing_from_cache(): void
    {
        $this->cache->set('first', rows(row(int_entry('id', 1))));
        $this->cache->set('second', rows(row(int_entry('id', 2))));

        $this->cache->delete('second');

        static::assertTrue($this->cache->has('first'));
        static::assertFalse($this->cache->has('second'));
    }

    public function test_removing_non_existing_cache_key(): void
    {
        $this->cache->delete('non-existing');

        static::assertFalse($this->cache->has('non-existing'));
    }
}
