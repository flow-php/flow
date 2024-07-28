<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{config_builder, df, from_cache};
use Flow\ETL\Cache\RowsCache\PSRSimpleCache;
use Flow\ETL\Config;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\Filesystem\{FileStatus};
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class CacheTest extends IntegrationTestCase
{
    public function test_cache() : void
    {
        $rows = df()
            ->read(new FakeExtractor($rowsets = 20))
            ->batchSize(2)
            ->cache('test_etl_cache')
            ->fetch();

        self::assertCount($rowsets, $rows);
        $cacheContent = $this->fs()->list($this->cacheDir->suffix('/**/*'));

        $files = \array_map(
            fn (FileStatus $p) => $p->path->basename(),
            \iterator_to_array($cacheContent)
        );

        self::assertContains(
            'test_etl_cache.php.cache',
            $files,
            'Cache Files: ' . \implode(', ', $files)
        );
    }

    public function test_psr_cache() : void
    {
        $adapter = new PSRSimpleCache(new Psr16Cache(new ArrayAdapter()));

        df(config_builder()->cache($adapter)->build())
            ->read(new FakeExtractor($rowsets = 20))
            ->batchSize(2)
            ->cache('test_etl_cache')
            ->run();

        $cachedRows = df(Config::builder()->cache($adapter)->build())->from(from_cache('test_etl_cache'))->fetch();

        self::assertCount($rowsets, $cachedRows);

        $adapter->remove('test_etl_cache');
        self::assertCount(0, \iterator_to_array($adapter->get('test_etl_cache')));
    }
}
