<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache\RowCache;

use function Flow\ETL\DSL\{array_to_rows, filesystem_row_cache};
use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\Serializer\{Base64Serializer, CompressingSerializer, NativePHPSerializer};

final class FilesystemCacheTest extends IntegrationTestCase
{
    public function test_caching_rows() : void
    {
        $cache = filesystem_row_cache(
            $this->cacheDir,
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
        );

        $cache->set('test', array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ]));

        $rows = \iterator_to_array($cache->get('test'));

        self::assertCount(3, $rows);
        self::assertEquals(['id' => 1], $rows[0]->toArray());
        self::assertEquals(['id' => 2], $rows[1]->toArray());
        self::assertEquals(['id' => 3], $rows[2]->toArray());

        $cache->remove('test');

        self::assertCount(0, \iterator_to_array($cache->get('test')));
    }
}
