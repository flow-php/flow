<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\read;
use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\Config;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class CacheTest extends IntegrationTestCase
{
    public function test_cache() : void
    {
        read(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache')
            ->run();

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        $this->assertContains('test_etl_cache', $cacheContent);
    }

    public function test_psr_cache() : void
    {
        read(
            new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2),
            Config::builder()->cache($cache = new PSRSimpleCache(new Psr16Cache(new ArrayAdapter())))->build()
        )
            ->cache('test_etl_cache')
            ->run();

        $cachedRows = read(from_cache('test_etl_cache'), Config::builder()->cache($cache)->build())->fetch();

        $this->assertCount($rowsets * $rows, $cachedRows);

        $cache->clear('test_etl_cache');
        $this->assertCount(0, \iterator_to_array($cache->read('test_etl_cache')));
    }
}
