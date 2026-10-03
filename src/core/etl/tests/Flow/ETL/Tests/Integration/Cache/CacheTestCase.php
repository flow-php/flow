<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Override;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
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

        $cache->set('index', $index->toRows(new PhpBackend()));

        static::assertTrue($cache->has('index'));

        $indexRows = $cache->get('index');

        static::assertInstanceOf(Rows::class, $indexRows);
        static::assertEquals($index, CacheIndex::fromRows('index', $indexRows));
    }

    public function test_caching_rows(): void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('rows'));

        $cache->set(
            'rows',
            $rows = array_to_rows([['name' => 'John'], ['name' => 'Jane']], schema(str_schema('name'))),
        );

        static::assertTrue($cache->has('rows'));

        static::assertEquals($rows, $cache->get('rows'));
    }

    public function test_caching_schema(): void
    {
        $cache = $this->cache();

        $cache->set(
            'rows',
            $rows = array_to_rows([['id' => 1, 'name' => 'John']], schema(int_schema('id'), str_schema('name'))),
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

        $cache->set('index', (new CacheIndex('index'))->toRows(new PhpBackend()));
        $cache->set('row', array_to_rows([['name' => 'John']], schema(str_schema('name'))));
        $cache->set('rows', array_to_rows([['name' => 'John'], ['name' => 'Jane']], schema(str_schema('name'))));

        $cache->clear();

        static::assertFalse($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertFalse($cache->has('rows'));
    }

    public function test_clearing_cache_removes_schemas(): void
    {
        $cache = $this->cache();

        $cache->set('rows', array_to_rows([['name' => 'John']], schema(str_schema('name'))));

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

        $cache->set('index', (new CacheIndex('index'))->toRows(new PhpBackend()));
        $cache->set('row', array_to_rows([['name' => 'John']], schema(str_schema('name'))));
        $cache->set('rows', array_to_rows([['name' => 'John'], ['name' => 'Jane']], schema(str_schema('name'))));

        $cache->delete('row');

        static::assertTrue($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertTrue($cache->has('rows'));
    }

    public function test_removing_from_cache_removes_its_schema(): void
    {
        $cache = $this->cache();

        $cache->set('row', array_to_rows([['name' => 'John']], schema(str_schema('name'))));
        $cache->set('rows', $rows = array_to_rows([['id' => 1]], schema(int_schema('id'))));

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
