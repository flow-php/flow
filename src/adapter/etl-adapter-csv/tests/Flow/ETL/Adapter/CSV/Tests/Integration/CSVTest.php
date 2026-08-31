<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\write_with_retries;
use function implode;
use function mkdir;
use function unlink;

final class CSVTest extends FlowTestCase
{
    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_header_and_body_follow_the_schema_order_not_the_row_key_order(): void
    {
        // R10: the Schema owns column order and rows are never rekeyed, so the writer reads the order
        // off the Schema - the first row's key order must not decide the header
        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('name')),
                row(['name' => 'a', 'id' => 1]),
                row(['id' => 2, 'name' => 'b']),
            )))
            ->load(to_csv($path = __DIR__ . '/var/test_schema_order.csv')->saveMode(overwrite()))
            ->run();

        static::assertSame(implode(PHP_EOL, ['id,name', '1,a', '2,b', '']), file_get_contents($path));

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_loading_csv_rows_of_decreasing_length_across_multiple_batches(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'value' => 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
                ['id' => 2, 'value' => 'b'],
                ['id' => 3, 'value' => 'cc'],
                ['id' => 4, 'value' => 'd'],
            ]))
            ->batchSize(2)
            ->load(
                to_csv($path = __DIR__ . '/var/test_loading_csv_rows_of_decreasing_length.csv')->saveMode(overwrite()),
            )
            ->run();

        static::assertSame(
            implode(PHP_EOL, ['id,value', '1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '2,b', '3,cc', '4,d', '']),
            file_get_contents($path),
        );

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_loading_csv_files(): void
    {
        df()
            ->read(new FakeExtractor(100))
            ->drop('array', 'list', 'map', 'struct', 'object', 'enum', 'list_of_datetimes')
            ->withEntry('datetime', ref('datetime')->dateFormat('Y-m-d H:i:s'))
            ->load(to_csv($path = __DIR__ . '/var/test_loading_csv_files.csv')->saveMode(overwrite()))
            ->run();

        static::assertEquals(100, df()->read(from_csv($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_retry_loader_publishes_csv_under_overwrite(): void
    {
        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(2)
            ->write(write_with_retries(
                to_csv($path = __DIR__ . '/var/test_retry_loader_overwrite.csv')->saveMode(overwrite()),
            ))
            ->run();

        static::assertFileExists($path);
        static::assertSame(4, df()->read(from_csv($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_transformation_loader_writes_all_batches_to_csv(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_csv($path = __DIR__ . '/var/test_transformation_loader.csv')->saveMode(overwrite()),
            ))
            ->run();

        $rows = df()->read(from_csv($path))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_writing_and_reading_csv_files_with_partition_placeholders(): void
    {
        $dir = __DIR__ . '/var/test_writing_and_reading_csv_files_with_partition_placeholders';

        df()
            ->read(from_array([
                ['year' => '2024', 'name' => '123456-PL', 'total' => 100],
                ['year' => '2024', 'name' => '789-DE', 'total' => 200],
                ['year' => '2025', 'name' => '555-FR', 'total' => 300],
            ]))
            ->load(to_csv($dir . '/{name}.csv')->saveMode(overwrite())->partitionBy(partition_by('year', 'name')))
            ->run();

        static::assertFileExists($dir . '/year=2024/123456-PL.csv');
        static::assertFileExists($dir . '/year=2024/789-DE.csv');
        static::assertFileExists($dir . '/year=2025/555-FR.csv');

        $rows = df()
            ->read(from_csv($dir . '/year=*/{name}.csv'))
            ->sortBy([ref('total')])
            ->fetch();

        static::assertCount(3, $rows);
        static::assertEquals(['2024', '2024', '2025'], $rows->reduceToArray(ref('year')));
        static::assertEquals(['123456-PL', '789-DE', '555-FR'], $rows->reduceToArray(ref('name')));

        $prunedRows = df()
            ->read(from_csv($dir . '/year=*/{name}.csv'))
            ->filterPartitions(ref('name')->equals(lit('789-DE')))
            ->fetch();

        static::assertCount(1, $prunedRows);
        static::assertEquals(['789-DE'], $prunedRows->reduceToArray(ref('name')));
    }

    /**
     * https://github.com/flow-php/flow/issues/2238
     */
    public function test_writing_csv_files_with_last_partition_as_file_name(): void
    {
        $output = __DIR__ . '/var/test_writing_csv_files_with_last_partition_as_file_name/output';

        df()
            ->read(from_array([
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '123456-PL', 'total' => 100],
                ['order-year' => '2024', 'order-month' => '03', 'order-name' => '789-DE', 'total' => 200],
                ['order-year' => '2025', 'order-month' => '01', 'order-name' => '555-FR', 'total' => 300],
            ]))
            ->load(
                to_csv($output . '/{order-name}.csv')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('order-year', 'order-month', 'order-name')),
            )
            ->run();

        static::assertFileExists($output . '/order-year=2024/order-month=03/123456-PL.csv');
        static::assertFileExists($output . '/order-year=2024/order-month=03/789-DE.csv');
        static::assertFileExists($output . '/order-year=2025/order-month=01/555-FR.csv');
        static::assertFileDoesNotExist($output . '/order-year=2024/order-month=03/order-name=123456-PL');
    }
}
