<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Flow\ETL\Adapter\Doctrine\DbalBulkLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\ArrayExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Double\Stub\TransformTestData;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\ETL;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class DbalQueryExtractorTest extends IntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_dbal_extractor_test');

        ETL::extract(
            new ArrayExtractor(
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            )
        )->transform(
            new TransformTestData()
        )->load(
            DbalBulkLoader::insert($this->pgsqlDatabaseContext->connection(), $bulkSize = 10, $table)
        );

        ETL::extract(
            new DbalQueryExtractor(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id"
            )
        )->load(
            $loader = new class implements Loader {
                public array $data = [];

                public function load(Rows $rows) : void
                {
                    $this->data = $rows->toArray();
                }
            }
        );

        $this->assertSame(
            [
                ['row' => ['id' => 1, 'name' => 'Name One', 'description' => 'Description One']],
                ['row' => ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two']],
                ['row' => ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three']],
            ],
            $loader->data
        );
    }
}
