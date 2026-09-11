<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache\Implementation;

use Flow\ETL\Cache\Implementation\ApcuCache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function apcu_enabled;
use function apcu_store;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

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
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'John']),
                row(['id' => 2, 'name' => 'Jane']),
            ),
        );

        static::assertTrue($this->cache->has('rows'));
        static::assertEquals($rows, $this->cache->get('rows'));
    }

    public function test_caching_schema(): void
    {
        $this->cache->set(
            'rows',
            $rows = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'John'])),
        );

        static::assertEquals($rows->schema(), $this->cache->schema('rows'));
    }

    public function test_checking_on_non_existing_cache_key(): void
    {
        static::assertFalse($this->cache->has('non-existing'));
    }

    public function test_clearing_cache_removes_only_its_own_namespace(): void
    {
        $this->cache->set('rows', rows(schema(int_schema('id')), row(['id' => 1])));
        $this->otherCache->set('rows', rows(schema(int_schema('id')), row(['id' => 2])));

        $this->cache->clear();

        static::assertFalse($this->cache->has('rows'));
        static::assertTrue($this->otherCache->has('rows'));
    }

    public function test_clearing_cache_removes_schemas(): void
    {
        $this->cache->set('rows', rows(schema(int_schema('id')), row(['id' => 1])));

        $this->cache->clear();

        $this->expectException(KeyNotInCacheException::class);

        $this->cache->schema('rows');
    }

    public function test_getting_corrupted_schema_entry(): void
    {
        apcu_store('flow_php_cache_test:rows:schema', 'not a schema');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cached schema for key "rows" is corrupted or was not written by ApcuCache.');

        $this->cache->schema('rows');
    }

    public function test_getting_non_existing_cache_key(): void
    {
        $this->expectException(KeyNotInCacheException::class);

        $this->cache->get('non-existing');
    }

    public function test_getting_schema_of_non_existing_cache_key(): void
    {
        $this->expectException(KeyNotInCacheException::class);

        $this->cache->schema('non-existing');
    }

    public function test_removing_from_cache(): void
    {
        $this->cache->set('first', rows(schema(int_schema('id')), row(['id' => 1])));
        $this->cache->set('second', rows(schema(int_schema('id')), row(['id' => 2])));

        $this->cache->delete('second');

        static::assertTrue($this->cache->has('first'));
        static::assertFalse($this->cache->has('second'));
    }

    public function test_removing_from_cache_removes_its_schema(): void
    {
        $this->cache->set('first', $rows = rows(schema(int_schema('id')), row(['id' => 1])));
        $this->cache->set('second', rows(schema(int_schema('id')), row(['id' => 2])));

        $this->cache->delete('second');

        static::assertEquals($rows->schema(), $this->cache->schema('first'));

        $this->expectException(KeyNotInCacheException::class);

        $this->cache->schema('second');
    }

    public function test_removing_non_existing_cache_key(): void
    {
        $this->cache->delete('non-existing');

        static::assertFalse($this->cache->has('non-existing'));
    }

    public function test_schema_of_an_entry_without_a_stored_schema_is_a_cache_miss(): void
    {
        apcu_store('flow_php_cache_test:orphan', rows(schema(int_schema('id')), row(['id' => 1])));

        static::assertTrue($this->cache->has('orphan'));

        $this->expectException(KeyNotInCacheException::class);

        $this->cache->schema('orphan');
    }
}
