<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use PHPUnit\Framework\TestCase;

final class MetadataTest extends TestCase
{
    public function test_column_orders_round_trip_per_leaf(): void
    {
        $schema = Schema::with(
            FlatColumn::string('name'),
            NestedColumn::struct('point', [FlatColumn::double('x'), FlatColumn::int64('y')]),
        );
        $read = Metadata::fromThrift(
            (new Metadata(
                $schema,
                new RowGroups([]),
                0,
                1,
                null,
                [
                    ColumnOrder::TypeDefined,
                    ColumnOrder::Ieee754TotalOrder,
                    ColumnOrder::TypeDefined,
                ],
            ))->toThrift(),
        );

        static::assertSame(ColumnOrder::TypeDefined, $read->columnOrder($schema->getFlat('name')));
        static::assertSame(ColumnOrder::Ieee754TotalOrder, $read->columnOrder($schema->getFlat('point.x')));
        static::assertSame(ColumnOrder::TypeDefined, $read->columnOrder($schema->getFlat('point.y')));
    }

    public function test_footer_without_column_orders_is_absent(): void
    {
        $schema = Schema::with(FlatColumn::string('name'));
        static::assertSame(
            ColumnOrder::Absent,
            Metadata::fromThrift(
                (new Metadata($schema, new RowGroups([]), 0, 1, null))->toThrift(),
            )->columnOrder($schema->getFlat('name')),
        );
    }

    public function test_footer_with_fewer_column_orders_than_leaves_is_refused(): void
    {
        $schema = Schema::with(FlatColumn::string('a'), FlatColumn::string('b'));
        $thrift = (new Metadata($schema, new RowGroups([]), 0, 1, null))->toThrift();
        $thrift->column_orders = [ColumnOrder::TypeDefined->toThrift()];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Footer holds 1 column orders for 2 leaf columns');

        Metadata::fromThrift($thrift);
    }

    public function test_column_order_of_a_column_outside_the_schema_is_refused(): void
    {
        $metadata = new Metadata(
            Schema::with(FlatColumn::string('a')),
            new RowGroups([]),
            0,
            1,
            null,
            [
                ColumnOrder::TypeDefined,
            ],
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "b" is not a leaf of the schema');

        $metadata->columnOrder(FlatColumn::string('b'));
    }

    public function test_an_unknown_column_order_is_written_back_as_unknown(): void
    {
        $schema = Schema::with(FlatColumn::string('a'), FlatColumn::double('b'));
        $read = Metadata::fromThrift(
            (new Metadata(
                $schema,
                new RowGroups([]),
                0,
                1,
                null,
                [
                    ColumnOrder::Unknown,
                    ColumnOrder::Ieee754TotalOrder,
                ],
            ))->toThrift(),
        );

        static::assertSame(ColumnOrder::Unknown, $read->columnOrder($schema->getFlat('a')));
        static::assertSame(ColumnOrder::Ieee754TotalOrder, $read->columnOrder($schema->getFlat('b')));
    }
}
