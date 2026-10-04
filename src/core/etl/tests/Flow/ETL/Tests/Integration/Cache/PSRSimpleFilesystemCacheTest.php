<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\Floe\FloeSerializer;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PSRSimpleFilesystemCacheTest extends CacheTestCase
{
    public function test_torn_entry_is_treated_as_a_cache_miss(): void
    {
        $psr = new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-simple-file-cache'));
        $cache = new PSRSimpleCache($psr, new FloeSerializer(new AdaptiveBackend()));
        $cache->set('torn', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $psr->set('torn', 'not a valid floe payload');

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('torn');
    }

    public function test_schema_of_an_entry_without_a_stored_schema_is_a_cache_miss(): void
    {
        $psr = new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-simple-file-cache'));
        $cache = new PSRSimpleCache($psr, new FloeSerializer(new AdaptiveBackend()));
        $cache->set('orphan', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $psr->delete('orphan.schema');

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('orphan');
    }

    public function test_torn_schema_is_treated_as_a_cache_miss(): void
    {
        $psr = new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-simple-file-cache'));
        $cache = new PSRSimpleCache($psr, new FloeSerializer(new AdaptiveBackend()));
        $cache->set('torn-schema', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $psr->set('torn-schema.schema', 'not a valid schema payload');

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('torn-schema');
    }

    protected function cache(): Cache
    {
        return new PSRSimpleCache(
            new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-simple-file-cache')),
            new FloeSerializer(new AdaptiveBackend()),
        );
    }
}
