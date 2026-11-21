<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\MultisetDigest;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\multiset_hash;
use function Flow\ETL\DSL\schema;
use function hash;
use function unpack;

final class DatasetHashTest extends FlowIntegrationTestCase
{
    public const array ROWS = [
        ['id' => 1, 'price' => 1, 'payload' => '{"a":1}'],
        ['id' => 2, 'price' => 1.5, 'payload' => '{"a": 2}'],
        ['id' => 3, 'price' => 2, 'payload' => '[]'],
    ];

    public function test_the_dataset_hash_does_not_depend_on_the_batch_size(): void
    {
        static::assertSame(
            '72c000bf6397e932faa3a3f30eeaa6db',
            df()
                ->read(from_array(self::ROWS)->withBatchSize(1))
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash(),
        );
        static::assertSame(
            '72c000bf6397e932faa3a3f30eeaa6db',
            df()->read(from_array(self::ROWS))->run(analyze: analyze()->withDatasetHash())->datasetHash(),
        );
    }

    public function test_the_dataset_hash_equals_the_multiset_hash_aggregate(): void
    {
        static::assertSame(
            [['_multiset_hash' => '72c000bf6397e932faa3a3f30eeaa6db']],
            df()->read(from_array(self::ROWS))->aggregate([multiset_hash()])->fetch()->toArray(),
        );
    }

    public function test_rows_without_columns_hash_as_empty_rows(): void
    {
        /** @var array{1: int, 2: int} $lanes */
        $lanes = unpack('J2', hash('xxh128', '', true));
        $empty = new MultisetDigest($lanes[2], $lanes[1]);

        static::assertSame(
            $empty->plus($empty)->plus($empty)->hex(),
            df()
                ->read(from_array(self::ROWS))
                ->drop('id', 'price', 'payload')
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash(),
        );
    }

    public function test_an_empty_dataset_hashes_to_zero(): void
    {
        static::assertSame(
            '00000000000000000000000000000000',
            df()
                ->read(from_array([])->withSchema(schema(int_schema('id'))))
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash(),
        );
    }
}
