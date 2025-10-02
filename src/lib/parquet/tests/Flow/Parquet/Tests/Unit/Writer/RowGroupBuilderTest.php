<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Dremel\{DremelShredder, RowGroupContainer};
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions, RowGroup, Schema};
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};
use Flow\Parquet\Writer\{RowGroupBuilder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RowGroupBuilderTest extends TestCase
{
    public static function compression_types_provider() : \Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function page_check_interval_provider() : \Generator
    {
        yield 'small interval' => [10];
        yield 'medium interval' => [100];
        yield 'large interval' => [1000];
    }

    public static function row_group_size_provider() : \Generator
    {
        yield 'small row group' => [124];
        yield 'medium row group' => [512];
        yield 'large row group' => [1024];
    }

    public static function schema_types_provider() : \Generator
    {
        yield 'single flat column' => [
            Schema::with(FlatColumn::int32('id')),
            'single integer column schema',
        ];

        yield 'multiple flat columns' => [
            Schema::with(
                FlatColumn::int32('id'),
                FlatColumn::string('name'),
                FlatColumn::boolean('active')
            ),
            'multiple flat columns schema',
        ];

        yield 'nested column' => [
            Schema::with(
                FlatColumn::int32('id'),
                NestedColumn::struct('nested', [
                    FlatColumn::int64('value'),
                    FlatColumn::string('label'),
                ])
            ),
            'schema with nested column',
        ];
    }

    public function test_add_multiple_rows() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        for ($i = 0; $i < 5; $i++) {
            $builder->addRow(['id' => $i * 10]);
        }

        self::assertSame(5, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_add_row_increments_rows_count() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertSame(0, $builder->rowsCount());

        $builder->addRow(['id' => 42]);

        self::assertSame(1, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_add_row_with_mixed_data_types() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name'),
            FlatColumn::double('score'),
            FlatColumn::boolean('active')
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow([
            'id' => 1,
            'name' => 'test',
            'score' => 95.5,
            'active' => true,
        ]);

        self::assertSame(1, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_add_row_with_nested_data() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            NestedColumn::struct('nested', [
                FlatColumn::int64('value'),
                FlatColumn::string('label'),
            ])
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow([
            'id' => 1,
            'nested' => [
                'value' => 123456789,
                'label' => 'test_label',
            ],
        ]);

        self::assertSame(1, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_add_row_with_null_values() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name')
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 1, 'name' => null]);

        self::assertSame(1, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_add_row_with_partial_data() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name')
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 1]);

        self::assertSame(1, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());
    }

    public function test_constructor_initializes_correctly() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertInstanceOf(RowGroupBuilder::class, $builder);
        self::assertTrue($builder->isEmpty());
        self::assertSame(0, $builder->rowsCount());
        self::assertFalse($builder->isFull());
    }

    #[DataProvider('schema_types_provider')]
    public function test_constructor_with_different_schemas(Schema $schema, string $description) : void
    {
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));

        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertInstanceOf(RowGroupBuilder::class, $builder);
        self::assertTrue($builder->isEmpty());
        self::assertSame(0, $builder->rowsCount());
    }

    public function test_flush_creates_row_group_container() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 42]);

        $container = $builder->flush(0);

        self::assertInstanceOf(RowGroupContainer::class, $container);
        self::assertInstanceOf(RowGroup::class, $container->rowGroup);
        self::assertSame(1, $container->rowGroup->rowsCount());
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
        self::assertTrue($builder->isEmpty());
    }

    public function test_flush_resets_rows_count() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 42]);
        self::assertSame(1, $builder->rowsCount());

        $builder->flush(0);

        self::assertSame(0, $builder->rowsCount());
        self::assertTrue($builder->isEmpty());
    }

    public function test_flush_with_different_file_offsets() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $offsets = [0, 100, 1000, 50000];

        foreach ($offsets as $offset) {
            // Add fresh data before each flush
            $builder->addRow(['id' => $offset + 42]);
            $container = $builder->flush($offset);

            self::assertInstanceOf(RowGroupContainer::class, $container);
            self::assertNotEmpty($container->binaryBuffer);
        }
    }

    public function test_flush_with_empty_data() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $container = $builder->flush(0);

        self::assertInstanceOf(RowGroupContainer::class, $container);
        self::assertSame(0, $container->rowGroup->rowsCount());
        self::assertIsString($container->binaryBuffer);
    }

    public function test_flush_with_multiple_columns() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name')
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 1, 'name' => 'test']);
        $builder->addRow(['id' => 2, 'name' => 'another']);

        $container = $builder->flush(0);

        self::assertInstanceOf(RowGroupContainer::class, $container);
        self::assertSame(2, $container->rowGroup->rowsCount());
        self::assertCount(2, $container->rowGroup->columnChunks());
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_is_empty_initially_true() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertTrue($builder->isEmpty());
    }

    public function test_is_empty_returns_false_after_adding_row() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 42]);

        self::assertFalse($builder->isEmpty());
    }

    public function test_is_empty_returns_true_after_flush() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 42]);
        self::assertFalse($builder->isEmpty());

        $builder->flush(0);

        self::assertTrue($builder->isEmpty());
    }

    public function test_is_full_initially_false() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertFalse($builder->isFull());
    }

    #[DataProvider('row_group_size_provider')]
    public function test_is_full_respects_row_group_size_option(int $rowGroupSize) : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $options->set(Option::ROW_GROUP_SIZE_BYTES, $rowGroupSize);
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertFalse($builder->isFull());

        for ($i = 0; $i < 10_000; $i++) {
            $builder->addRow(['id' => $i]);

            if ($builder->isFull()) {
                self::assertTrue(true);

                return;
            }
        }

        self::fail('Expected builder to be full but it was not.');
    }

    #[DataProvider('page_check_interval_provider')]
    public function test_page_check_interval_triggers_page_closing(int $interval) : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $options->set(Option::PAGE_SIZE_CHECK_INTERVAL, $interval);
        $options->set(Option::PAGE_SIZE_BYTES, 1);
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        for ($i = 0; $i < $interval * 2; $i++) {
            $builder->addRow(['id' => $i]);
        }

        self::assertSame($interval * 2, $builder->rowsCount());
    }

    public function test_rows_count_increases_with_each_row() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        for ($i = 1; $i <= 5; $i++) {
            $builder->addRow(['id' => $i]);
            self::assertSame($i, $builder->rowsCount());
        }
    }

    public function test_rows_count_initially_zero() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        self::assertSame(0, $builder->rowsCount());
    }

    public function test_workflow_add_flush_cycle() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name')
        );
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 1, 'name' => 'first']);
        $builder->addRow(['id' => 2, 'name' => 'second']);
        $builder->addRow(['id' => 3, 'name' => 'third']);

        self::assertSame(3, $builder->rowsCount());
        self::assertFalse($builder->isEmpty());

        $container = $builder->flush(0);

        self::assertInstanceOf(RowGroupContainer::class, $container);
        self::assertSame(3, $container->rowGroup->rowsCount());
        self::assertTrue($builder->isEmpty());
        self::assertSame(0, $builder->rowsCount());
    }

    public function test_workflow_multiple_flush_cycles() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        for ($cycle = 0; $cycle < 3; $cycle++) {
            for ($i = 0; $i < 2; $i++) {
                $builder->addRow(['id' => $cycle * 10 + $i]);
            }

            $container = $builder->flush($cycle * 1000);

            self::assertInstanceOf(RowGroupContainer::class, $container);
            self::assertSame(2, $container->rowGroup->rowsCount());
            self::assertTrue($builder->isEmpty());
        }
    }

    #[DataProvider('compression_types_provider')]
    public function test_workflow_with_different_compressions(Compressions $compression) : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $options = new Options();
        $shredder = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
        $builder = new RowGroupBuilder($schema, $compression, $options, $shredder);

        $builder->addRow(['id' => 42]);

        $container = $builder->flush(0);

        self::assertInstanceOf(RowGroupContainer::class, $container);
        self::assertSame(1, $container->rowGroup->rowsCount());
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }
}
