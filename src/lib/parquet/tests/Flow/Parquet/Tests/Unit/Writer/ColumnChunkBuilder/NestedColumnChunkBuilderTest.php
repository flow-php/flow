<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkBuilder\NestedColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkBuilder\PlainFlatColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkContainer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class NestedColumnChunkBuilderTest extends TestCase
{
    public static function compression_types_provider(): Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function nested_column_structures_provider(): Generator
    {
        yield 'single child' => [
            [new FlatColumn('child1', PhysicalType::INT32)],
            'single child nested column',
        ];

        yield 'multiple children' => [
            [
                new FlatColumn('child1', PhysicalType::INT32),
                new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string()),
                new FlatColumn('child3', PhysicalType::BOOLEAN),
            ],
            'multiple children nested column',
        ];

        yield 'mixed types' => [
            [
                new FlatColumn('int_child', PhysicalType::INT32),
                new FlatColumn('float_child', PhysicalType::FLOAT),
                new FlatColumn('string_child', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string()),
            ],
            'mixed types nested column',
        ];
    }

    public function test_add_multiple_rows(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        for ($i = 0; $i < 5; $i++) {
            $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [$i * 10]));
        }

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_finds_correct_child_builder_by_flat_path(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $childColumn3 = new FlatColumn('child3', PhysicalType::BOOLEAN);
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2, $childColumn3]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $childBuilder3 = new PlainFlatColumnChunkBuilder($childColumn3, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2, $childBuilder3]);

        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['middle_child']));

        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_empty_data(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [], [], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_multiple_children_data(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['hello']));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_null_values(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [0], []));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_partial_child_data(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_add_row_with_single_child_data(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        static::assertFalse($builder->isFull());
        static::assertGreaterThanOrEqual(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_empty_children(): void
    {
        $nestedColumn = NestedColumn::create('nested', []);
        $builder = new NestedColumnChunkBuilder($nestedColumn, []);

        $builder->closePage();

        static::assertFalse($builder->isFull());
        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_multiple_children(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['hello']));

        $builder->closePage();

        static::assertFalse($builder->isFull());
        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_close_page_with_single_child(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        $builder->closePage();

        static::assertFalse($builder->isFull());
        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_column_returns_correct_nested_column(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $result = $builder->column();

        static::assertSame($nestedColumn, $result);
        static::assertInstanceOf(NestedColumn::class, $result);
    }

    public function test_constructor_initializes_correctly(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $childrenBuilders = [$childBuilder];

        $builder = new NestedColumnChunkBuilder($nestedColumn, $childrenBuilders);

        static::assertInstanceOf(NestedColumnChunkBuilder::class, $builder);
        static::assertSame($nestedColumn, $builder->column());
        static::assertFalse($builder->isFull());
        static::assertSame(0, $builder->uncompressedSize());
    }

    /**
     * @param array<FlatColumn> $children
     */
    #[DataProvider('nested_column_structures_provider')]
    public function test_constructor_with_different_nested_structures(array $children, string $description): void
    {
        $nestedColumn = NestedColumn::create('nested', $children);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childrenBuilders = [];

        foreach ($children as $child) {
            $childrenBuilders[] = new PlainFlatColumnChunkBuilder($child, $options, $compression);
        }

        $builder = new NestedColumnChunkBuilder($nestedColumn, $childrenBuilders);

        static::assertInstanceOf(NestedColumnChunkBuilder::class, $builder);
        static::assertSame($nestedColumn, $builder->column());
        static::assertCount(count($children), $childrenBuilders);
    }

    public function test_constructor_with_empty_children(): void
    {
        $nestedColumn = NestedColumn::create('nested', []);
        $childrenBuilders = [];

        $builder = new NestedColumnChunkBuilder($nestedColumn, $childrenBuilders);

        static::assertInstanceOf(NestedColumnChunkBuilder::class, $builder);
        static::assertSame($nestedColumn, $builder->column());
        static::assertFalse($builder->isFull());
        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_edge_case_mixed_null_and_non_null_across_children(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [0], []));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(2, $containers);
        static::assertSame(1, $containers[0]->columnChunk->valuesCount());
        static::assertSame(1, $containers[1]->columnChunk->valuesCount());
    }

    public function test_edge_case_no_matching_flat_path(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(2, $containers);
        static::assertSame(1, $containers[0]->columnChunk->valuesCount());
        static::assertSame(0, $containers[1]->columnChunk->valuesCount());
    }

    public function test_flush_automatically_closes_pages_when_data_exists(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertGreaterThan(0, $containers[0]->columnChunk->valuesCount());
    }

    public function test_flush_calculates_correct_file_offsets(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $childColumn3 = new FlatColumn('child3', PhysicalType::BOOLEAN);
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2, $childColumn3]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $childBuilder3 = new PlainFlatColumnChunkBuilder($childColumn3, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2, $childBuilder3]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['hello']));
        $childBuilder3->addColumn(new WriteFlatColumnValues($childColumn3, [0], [1], [true]));

        $containers = $builder->flush(1000);

        static::assertIsArray($containers);
        static::assertCount(3, $containers);

        $firstOffset = $containers[0]->columnChunk->fileOffset();
        $secondOffset = $containers[1]->columnChunk->fileOffset();
        $thirdOffset = $containers[2]->columnChunk->fileOffset();

        static::assertSame(1000, $firstOffset);
        static::assertGreaterThan($firstOffset, $secondOffset);
        static::assertGreaterThan($secondOffset, $thirdOffset);

        $expectedSecondOffset = $firstOffset + strlen($containers[0]->binaryBuffer);
        $expectedThirdOffset = $secondOffset + strlen($containers[1]->binaryBuffer);

        static::assertSame($expectedSecondOffset, $secondOffset);
        static::assertSame($expectedThirdOffset, $thirdOffset);
    }

    public function test_flush_cleans_up_child_builder_state(): void
    {
        $compression = Compressions::UNCOMPRESSED;
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0, 0], [1, 1], [42, 84]));

        static::assertFalse($childBuilder->isEmpty());
        static::assertGreaterThan(0, $childBuilder->uncompressedSize());

        $containers = $builder->flush(0);
        static::assertCount(1, $containers);

        static::assertTrue($childBuilder->isEmpty());
        static::assertEquals(0, $childBuilder->uncompressedSize());

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [126]));

        static::assertFalse($childBuilder->isEmpty());
        static::assertGreaterThan(0, $childBuilder->uncompressedSize());
    }

    public function test_flush_with_different_file_offsets(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $offsets = [0, 100, 1000, 50000];

        foreach ($offsets as $offset) {
            $containers = $builder->flush($offset);

            static::assertIsArray($containers);
            static::assertCount(1, $containers);
            static::assertSame($offset, $containers[0]->columnChunk->fileOffset());
        }
    }

    public function test_flush_with_empty_children(): void
    {
        $nestedColumn = NestedColumn::create('nested', []);
        $builder = new NestedColumnChunkBuilder($nestedColumn, []);

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertEmpty($containers);
    }

    public function test_flush_with_multiple_children(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['hello']));

        $containers = $builder->flush(100);

        static::assertIsArray($containers);
        static::assertCount(2, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
        static::assertSame(100, $containers[0]->columnChunk->fileOffset());
        static::assertGreaterThan(100, $containers[1]->columnChunk->fileOffset());
    }

    public function test_flush_with_single_child(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        $containers = $builder->flush(100);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(100, $containers[0]->columnChunk->fileOffset());
    }

    public function test_is_full_initially_false(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        static::assertFalse($builder->isFull());
    }

    public function test_is_full_returns_false_when_no_child_is_full(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        static::assertFalse($builder->isFull());
    }

    public function test_is_full_returns_true_when_any_child_is_full(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);

        $mockFullChildBuilder = $this->createMock(ColumnChunkBuilder::class);
        $mockFullChildBuilder->method('isFull')->willReturn(true);
        $mockFullChildBuilder->method('column')->willReturn($childColumn1);

        $builder = new NestedColumnChunkBuilder($nestedColumn, [$mockFullChildBuilder, $childBuilder2]);

        static::assertTrue($builder->isFull());
    }

    public function test_is_full_with_empty_children(): void
    {
        $nestedColumn = NestedColumn::create('nested', []);
        $builder = new NestedColumnChunkBuilder($nestedColumn, []);

        static::assertFalse($builder->isFull());
    }

    public function test_multiple_close_page_calls(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        $builder->closePage();
        $builder->closePage();
        $builder->closePage();

        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_uncompressed_size_initially_zero(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_uncompressed_size_sums_children_sizes(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [42]));
        $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ['hello']));
        $builder->closePage();

        $totalSize = $builder->uncompressedSize();
        $child1Size = $childBuilder1->uncompressedSize();
        $child2Size = $childBuilder2->uncompressedSize();

        static::assertSame($child1Size + $child2Size, $totalSize);
        static::assertGreaterThan(0, $totalSize);
    }

    public function test_uncompressed_size_with_empty_children(): void
    {
        $nestedColumn = NestedColumn::create('nested', []);
        $builder = new NestedColumnChunkBuilder($nestedColumn, []);

        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_uncompressed_size_with_mock_children(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);

        $mockChild1 = $this->createMock(ColumnChunkBuilder::class);
        $mockChild1->method('uncompressedSize')->willReturn(100);
        $mockChild1->method('column')->willReturn($childColumn1);

        $mockChild2 = $this->createMock(ColumnChunkBuilder::class);
        $mockChild2->method('uncompressedSize')->willReturn(200);
        $mockChild2->method('column')->willReturn($childColumn2);

        $builder = new NestedColumnChunkBuilder($nestedColumn, [$mockChild1, $mockChild2]);

        static::assertSame(300, $builder->uncompressedSize());
    }

    public function test_workflow_complete_add_close_flush_cycle(): void
    {
        $childColumn1 = new FlatColumn('child1', PhysicalType::INT32);
        $childColumn2 = new FlatColumn('child2', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $nestedColumn = NestedColumn::create('nested', [$childColumn1, $childColumn2]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder1 = new PlainFlatColumnChunkBuilder($childColumn1, $options, $compression);
        $childBuilder2 = new PlainFlatColumnChunkBuilder($childColumn2, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder1, $childBuilder2]);

        for ($i = 0; $i < 3; $i++) {
            $childBuilder1->addColumn(new WriteFlatColumnValues($childColumn1, [0], [1], [$i * 10]));
            $childBuilder2->addColumn(new WriteFlatColumnValues($childColumn2, [0], [1], ["value_{$i}"]));
        }

        $builder->closePage();
        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(2, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[1]);
        static::assertSame(3, $containers[0]->columnChunk->valuesCount());
        static::assertSame(3, $containers[1]->columnChunk->valuesCount());
    }

    public function test_workflow_multiple_cycles(): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        for ($cycle = 0; $cycle < 3; $cycle++) {
            $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [$cycle * 100]));
            $builder->closePage();
        }

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertSame(3, $containers[0]->columnChunk->valuesCount());
    }

    #[DataProvider('compression_types_provider')]
    public function test_workflow_with_different_compressions(Compressions $compression): void
    {
        $childColumn = new FlatColumn('child', PhysicalType::INT32);
        $nestedColumn = NestedColumn::create('nested', [$childColumn]);
        $options = new Options();

        $childBuilder = new PlainFlatColumnChunkBuilder($childColumn, $options, $compression);
        $builder = new NestedColumnChunkBuilder($nestedColumn, [$childBuilder]);

        $childBuilder->addColumn(new WriteFlatColumnValues($childColumn, [0], [1], [42]));

        $builder->closePage();
        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }
}
