<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;
use Override;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

abstract class CacheTestCase extends FlowIntegrationTestCase
{
    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        $this->cache()->clear();
    }

    #[Override]
    protected function tearDown(): void
    {
        parent::tearDown();

        $this->cache()->clear();
    }

    public function test_caching_index(): void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('index'));

        $index = new CacheIndex('index');
        $index->add('chunk-1');
        $index->add('chunk-2');

        $cache->set('index', $index->toRows());

        static::assertTrue($cache->has('index'));

        $indexRows = $cache->get('index');

        static::assertInstanceOf(Rows::class, $indexRows);
        static::assertEquals($index, CacheIndex::fromRows('index', $indexRows));
    }

    public function test_caching_rows(): void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('rows'));

        $cache->set('rows', $rows = rows(schema(str_schema('name')), row(['name' => 'John']), row(['name' => 'Jane'])));

        static::assertTrue($cache->has('rows'));

        static::assertEquals($rows, $cache->get('rows'));
    }

    public function test_caching_partitioned_rows(): void
    {
        $cache = $this->cache();

        $cache->set(
            'partitioned',
            $rows = Rows::partitioned(
                schema(int_schema('id'), str_schema('country')),
                [row(['id' => 1, 'country' => 'PL'])],
                [new Partition('country', 'PL')],
            ),
        );

        static::assertTrue($cache->has('partitioned'));
        static::assertEquals($rows, $cache->get('partitioned'));
    }

    public function test_caching_schema(): void
    {
        $cache = $this->cache();

        $cache->set(
            'rows',
            $rows = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'John'])),
        );

        static::assertEquals($rows->schema(), $cache->schema('rows'));
    }

    public function test_checking_on_non_existing_cache_key(): void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('non-existing'));
    }

    public function test_clearing_cache(): void
    {
        $cache = $this->cache();

        $cache->set('index', (new CacheIndex('index'))->toRows());
        $cache->set('row', rows(schema(str_schema('name')), row(['name' => 'John'])));
        $cache->set('rows', rows(schema(str_schema('name')), row(['name' => 'John']), row(['name' => 'Jane'])));

        $cache->clear();

        static::assertFalse($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertFalse($cache->has('rows'));
    }

    public function test_clearing_cache_removes_schemas(): void
    {
        $cache = $this->cache();

        $cache->set('rows', rows(schema(str_schema('name')), row(['name' => 'John'])));

        $cache->clear();

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('rows');
    }

    public function test_getting_non_existing_cache_key(): void
    {
        $cache = $this->cache();

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('non-existing');
    }

    public function test_getting_schema_of_non_existing_cache_key(): void
    {
        $cache = $this->cache();

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('non-existing');
    }

    public function test_removing_from_cache(): void
    {
        $cache = $this->cache();

        $cache->set('index', (new CacheIndex('index'))->toRows());
        $cache->set('row', rows(schema(str_schema('name')), row(['name' => 'John'])));
        $cache->set('rows', rows(schema(str_schema('name')), row(['name' => 'John']), row(['name' => 'Jane'])));

        $cache->delete('row');

        static::assertTrue($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertTrue($cache->has('rows'));
    }

    public function test_removing_from_cache_removes_its_schema(): void
    {
        $cache = $this->cache();

        $cache->set('row', rows(schema(str_schema('name')), row(['name' => 'John'])));
        $cache->set('rows', $rows = rows(schema(int_schema('id')), row(['id' => 1])));

        $cache->delete('row');

        static::assertEquals($rows->schema(), $cache->schema('rows'));

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('row');
    }

    public function test_removing_non_existing_cache_key(): void
    {
        $cache = $this->cache();

        $cache->delete('non-existing');

        static::assertFalse($cache->has('non-existing'));
    }

    abstract protected function cache(): Cache;
}
