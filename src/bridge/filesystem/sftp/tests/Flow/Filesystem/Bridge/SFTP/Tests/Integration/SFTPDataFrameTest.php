<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_uuid;

final class SFTPDataFrameTest extends SFTPTestCase
{
    public function test_orders_written_as_csv_are_read_back_from_sftp(): void
    {
        $filesystem = $this->sftpContext()->filesystem();

        $report = data_frame()
            ->read(new FakeStaticOrdersExtractor(1_000))
            ->drop('address', 'notes', 'items')
            ->write(to_csv(path('sftp:///upload/orders.csv'), filesystem: $filesystem))
            ->run(analyze: true);

        static::assertSame(1_000, $report->statistics()->totalRows());

        $rows = data_frame()->read(from_csv(path('sftp:///upload/orders.csv'), filesystem: $filesystem))->fetch();

        static::assertCount(1_000, $rows);
        static::assertSame('user-0@example.com', $rows->first()->get('email'));
        static::assertSame('user-999@example.com', $rows->last()?->get('email'));
    }

    public function test_orders_written_as_parquet_survive_a_round_trip_with_every_entry_type(): void
    {
        $filesystem = $this->sftpContext()->filesystem();

        $report = data_frame()
            ->read(new FakeStaticOrdersExtractor(1_000))
            ->write(to_parquet(path('sftp:///upload/orders.parquet'), filesystem: $filesystem))
            ->run(analyze: true);

        static::assertSame(1_000, $report->statistics()->totalRows());

        $rows = data_frame()
            ->read(from_parquet(path('sftp:///upload/orders.parquet'), filesystem: $filesystem))
            ->fetch();

        static::assertCount(1_000, $rows);

        $first = $rows->first();

        static::assertSame(0, $first->get('index'));
        static::assertSame(
            '254d61c5-22c8-4407-83a2-76f1cab53af2',
            type_uuid()->assert($first->get('order_id'))->toString(),
        );
        static::assertSame(
            '2025-01-01 12:00:00',
            type_datetime()->assert($first->get('created_at'))->format('Y-m-d H:i:s'),
        );
        static::assertSame(
            ['street' => '123 Main St, Apt 0', 'city' => 'City ', 'zip' => '12345-0', 'country' => 'PL'],
            $first->get('address'),
        );
        static::assertSame(['Note 1 for order 0', 'Note 2 for order 0', 'Note 3 for order 0'], $first->get('notes'));
        static::assertEqualsWithDelta(
            [
                ['sku' => 'SKU_0001', 'quantity' => 1, 'price' => 0.14],
                ['sku' => 'SKU_0002', 'quantity' => 2, 'price' => 25.13],
            ],
            $first->get('items'),
            0.0001,
        );
    }

    public function test_reading_every_csv_matching_a_recursive_pattern(): void
    {
        $filesystem = $this->sftpContext()->filesystem();

        data_frame()
            ->read(new FakeStaticOrdersExtractor(100))
            ->drop('address', 'notes', 'items')
            ->write(to_csv(path('sftp:///upload/2024/01/orders.csv'), filesystem: $filesystem))
            ->run();

        data_frame()
            ->read(new FakeStaticOrdersExtractor(100))
            ->drop('address', 'notes', 'items')
            ->write(to_csv(path('sftp:///upload/2024/02/orders.csv'), filesystem: $filesystem))
            ->run();

        static::assertCount(
            200,
            data_frame()->read(from_csv(path('sftp:///upload/**/*.csv'), filesystem: $filesystem))->fetch(),
        );
    }

    public function test_a_dataset_larger_than_a_single_block_is_uploaded_in_parts(): void
    {
        $filesystem = $this->sftpContext()->filesystem(sftp_filesystem_options()->withBlockSize(256 * 1024));

        $report = data_frame()
            ->read(new FakeRandomOrdersExtractor(10_000))
            ->write(to_csv(path('sftp:///upload/orders.csv'), filesystem: $filesystem))
            ->run(analyze: true);

        static::assertSame(10_000, $report->statistics()->totalRows());
        static::assertGreaterThan(256 * 1024, $this->sftpContext()->sizeOf(path('sftp:///upload/orders.csv')));
        static::assertCount(
            10_000,
            data_frame()->read(from_csv(path('sftp:///upload/orders.csv'), filesystem: $filesystem))->fetch(),
        );
    }
}
