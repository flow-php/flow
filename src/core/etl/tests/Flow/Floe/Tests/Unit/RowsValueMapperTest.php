<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Footer;
use Flow\Floe\RowsValueMapper;
use Flow\Floe\Tests\Context\FloeFileContext;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class RowsValueMapperTest extends TestCase
{
    public function test_metadata_tags_a_row(): void
    {
        static::assertSame(
            [RowsValueMapper::VALUE_TYPE_KEY => 'row'],
            RowsValueMapper::metadataFor(row(int_entry('id', 1)))->normalize(),
        );
    }

    public function test_metadata_tags_rows(): void
    {
        static::assertSame(
            [RowsValueMapper::VALUE_TYPE_KEY => 'rows'],
            RowsValueMapper::metadataFor(rows(row(int_entry('id', 1))))->normalize(),
        );
    }

    public function test_partitions_from_follows_footer_order_without_order_metadata(): void
    {
        $footer = new Footer(1, 'test', [], [], [], ['country' => 'PL', 'year' => '2025'], 0, Metadata::empty());

        static::assertEquals(
            [new Partition('country', 'PL'), new Partition('year', '2025')],
            RowsValueMapper::partitionsFrom($footer),
        );
    }

    public function test_partitions_from_rejects_malformed_order_metadata(): void
    {
        $footer = new Footer(
            1,
            'test',
            [],
            [],
            [],
            ['country' => 'PL'],
            0,
            Metadata::with('flow.cache.partition_order', ['not' => 'a-list']),
        );

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('partition order metadata is malformed');

        RowsValueMapper::partitionsFrom($footer);
    }

    public function test_reconstruct_defaults_to_rows_without_value_type_metadata(): void
    {
        $footer = new Footer(1, 'test', [], [], [], [], 1, Metadata::empty());

        static::assertEquals(
            rows(row(int_entry('id', 1))),
            RowsValueMapper::reconstructFrom([row(int_entry('id', 1))], $footer),
        );
    }

    public function test_reconstruct_rejects_row_tagged_payload_without_rows(): void
    {
        $footer = new Footer(1, 'test', [], [], [], [], 0, Metadata::with(RowsValueMapper::VALUE_TYPE_KEY, 'row'));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('contained no rows');

        RowsValueMapper::reconstructFrom([], $footer);
    }

    public function test_reconstruct_empty_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://codec-empty.floe');
        $value = rows();

        $writer = new FloeWriter($filesystem);
        $writer->create($path, RowsValueMapper::metadataFor($value));
        $writer->write(RowsValueMapper::wrap($value));
        $writer->close();

        static::assertEquals($value, FloeFileContext::reconstruct($filesystem, $path));
    }

    public function test_reconstruct_partitioned_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://codec-partitioned.floe');
        $value = Rows::partitioned([row(int_entry('id', 1), str_entry('country', 'PL'))], [new Partition(
            'country',
            'PL',
        )]);

        $writer = new FloeWriter($filesystem);
        $writer->create($path, RowsValueMapper::metadataFor($value));
        $writer->write(RowsValueMapper::wrap($value));
        $writer->close();

        static::assertEquals($value, FloeFileContext::reconstruct($filesystem, $path));
    }

    public function test_reconstruct_preserves_non_alphabetical_partition_order(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://codec-partition-order.floe');
        $value = Rows::partitioned([row(
            int_entry('id', 1),
            str_entry('year', '2020'),
            str_entry('day', '15'),
            str_entry('month', '03'),
        )], [new Partition('year', '2020'), new Partition('day', '15'), new Partition('month', '03')]);

        $writer = new FloeWriter($filesystem);
        $writer->create($path, RowsValueMapper::metadataFor($value));
        $writer->write(RowsValueMapper::wrap($value));
        $writer->close();

        $result = FloeFileContext::reconstruct($filesystem, $path);

        static::assertInstanceOf(Rows::class, $result);
        static::assertSame(
            ['year', 'day', 'month'],
            array_map(static fn(Partition $p): string => $p->name, $result->partitions()->toArray()),
        );
        static::assertEquals($value, $result);
    }

    public function test_reconstruct_row(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://codec-row.floe');
        $value = row(int_entry('id', 1), str_entry('name', 'John'));

        $writer = new FloeWriter($filesystem);
        $writer->create($path, RowsValueMapper::metadataFor($value));
        $writer->write(RowsValueMapper::wrap($value));
        $writer->close();

        $result = FloeFileContext::reconstruct($filesystem, $path);

        static::assertInstanceOf(Row::class, $result);
        static::assertEquals($value, $result);
    }

    public function test_reconstruct_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://codec-rows.floe');
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        $writer = new FloeWriter($filesystem);
        $writer->create($path, RowsValueMapper::metadataFor($value));
        $writer->write(RowsValueMapper::wrap($value));
        $writer->close();

        $result = FloeFileContext::reconstruct($filesystem, $path);

        static::assertInstanceOf(Rows::class, $result);
        static::assertEquals($value, $result);
    }

    public function test_wrap_keeps_rows(): void
    {
        $value = rows(row(int_entry('id', 1)));

        static::assertSame($value, RowsValueMapper::wrap($value));
    }

    public function test_wrap_lifts_a_row_into_rows(): void
    {
        static::assertEquals(rows(row(int_entry('id', 1))), RowsValueMapper::wrap(row(int_entry('id', 1))));
    }
}
