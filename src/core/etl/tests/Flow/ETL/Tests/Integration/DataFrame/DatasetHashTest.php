<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\Filesystem\Partition;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\multiset_hash;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Floe\DSL\to_floe;
use function glob;
use function hash;
use function implode;
use function ksort;
use function sort;
use function strlen;
use function substr;
use function unpack;

use const GLOB_ONLYDIR;

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
                ->datasetHash()
                ?->total
                ->value,
        );
        static::assertSame(
            '72c000bf6397e932faa3a3f30eeaa6db',
            df()
                ->read(from_array(self::ROWS))
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash()
                ?->total
                ->value,
        );
    }

    public function test_the_dataset_hash_equals_the_multiset_hash_aggregate(): void
    {
        static::assertSame(
            [['_multiset_hash' => '72c000bf6397e932faa3a3f30eeaa6db']],
            df()->read(from_array(self::ROWS))->aggregate([multiset_hash()])->fetch()->toArray(),
        );
    }

    public function test_the_report_names_the_chosen_algorithm(): void
    {
        $hash = df()
            ->read(from_array([
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'DE'],
                ['id' => 3, 'country' => 'PL'],
            ]))
            ->run(analyze: analyze()->withDatasetHash(algorithm: HashAlgorithm::sha256))
            ->datasetHash();

        static::assertSame(HashAlgorithm::sha256, $hash?->algorithm);
        static::assertSame('cb6fbf9a3db645ba28221bc5cc601940', $hash->total->value);
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
                ->datasetHash()
                ?->total
                ->value,
        );
    }

    public function test_an_empty_dataset_hashes_to_zero(): void
    {
        static::assertSame(
            '00000000000000000000000000000000',
            df()
                ->read(from_array([])->withSchema(schema(int_schema('id'))))
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash()
                ?->total
                ->value,
        );
    }

    #[TestWith([1])]
    #[TestWith([100])]
    public function test_a_partitioned_write_gets_one_partition_hash_per_folder(int $batchSize): void
    {
        $output = $this->cacheDir->suffix('/orders');
        $schema = schema(date_schema('date'), str_schema('country'), int_schema('id'));
        $orders = [
            ['date' => new DateTimeImmutable('2026-11-20'), 'country' => 'PL', 'id' => 1],
            ['date' => new DateTimeImmutable('2026-11-20'), 'country' => 'DE', 'id' => 2],
            ['date' => new DateTimeImmutable('2026-11-21'), 'country' => 'PL', 'id' => 3],
            ['date' => new DateTimeImmutable('2026-11-20'), 'country' => 'PL', 'id' => 4],
        ];

        $hash = df()
            ->read(from_array($orders)->withSchema($schema)->withBatchSize($batchSize))
            ->write(
                to_floe($output->suffix('/orders.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(['date', 'country']),
            )
            ->run(analyze: analyze()->withDatasetHash([ref('date'), ref('country')]))
            ->datasetHash();
        $folders = [];

        foreach (glob($output->path() . '/*/*', GLOB_ONLYDIR) ?: [] as $folder) {
            $folders[] = substr($folder, strlen($output->path()) + 1);
        }

        $rows = [];

        foreach ($hash->partitions ?? [] as $partition) {
            $rows[implode('/', array_map(
                static fn(Partition $value): string => $value->segment(),
                $partition->partitions->toArray(),
            ))] = $partition->hash->rows;
        }

        sort($folders);
        ksort($rows);

        static::assertSame(
            ['date=2026-11-20/country=DE', 'date=2026-11-20/country=PL', 'date=2026-11-21/country=PL'],
            $folders,
        );
        static::assertSame(
            ['date=2026-11-20/country=DE' => 1, 'date=2026-11-20/country=PL' => 2, 'date=2026-11-21/country=PL' => 1],
            $rows,
        );
        static::assertEquals(
            df()
                ->read(from_array($orders)->withSchema($schema))
                ->run(analyze: analyze()->withDatasetHash())
                ->datasetHash()
                ?->total,
            $hash?->total,
        );
    }
}
