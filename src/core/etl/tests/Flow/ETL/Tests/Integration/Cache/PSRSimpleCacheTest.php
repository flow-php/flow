<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\{Row, Rows};
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class PSRSimpleCacheTest extends IntegrationTestCase
{
    public function test_saving_to_psr_simple_cache_implementation() : void
    {
        $cache = new PSRSimpleCache(
            new Psr16Cache(
                new FilesystemAdapter(directory: __DIR__ . '/var/flow-etl-cache-' . bin2hex(random_bytes(16)))
            ),
        );

        self::assertFalse($cache->has('test'));

        $cache->add('test', new Rows(Row::create(int_entry('id', 1))));
        $cache->add('test', new Rows(Row::create(int_entry('id', 2))));
        $cache->add('test', new Rows(Row::create(int_entry('id', 3))));

        self::assertCount(
            3,
            \iterator_to_array($cache->read('test'))
        );
        self::assertTrue($cache->has('test'));
    }
}
