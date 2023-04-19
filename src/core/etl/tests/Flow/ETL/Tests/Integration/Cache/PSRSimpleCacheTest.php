<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class PSRSimpleCacheTest extends IntegrationTestCase
{
    public function test_saving_to_psr_simple_cache_implementation() : void
    {
        $cache = new PSRSimpleCache(
            new Psr16Cache(
                new FilesystemAdapter(directory: \sys_get_temp_dir() . '/' . \uniqid('flow-etl-cache-', true))
            ),
        );

        $this->assertFalse($cache->has('test'));

        $cache->add('test', new Rows(Row::create(Entry::int('id', 1))));
        $cache->add('test', new Rows(Row::create(Entry::int('id', 2))));
        $cache->add('test', new Rows(Row::create(Entry::int('id', 3))));

        $this->assertCount(
            3,
            \iterator_to_array($cache->read('test'))
        );
        $this->assertTrue($cache->has('test'));
    }
}
