<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\FlatColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkBuilder\RLEDictionaryChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkBuilderFactory;
use Flow\Parquet\Writer\StatisticsOptions;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_values;

final class ColumnChunkBuilderFactoryTest extends TestCase
{
    public function test_create_builder_custom_encoding_overrides_automatic_selection(): void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::WRITER_VERSION, 2)->set(Option::COLUMNS_ENCODINGS, [
            'user_id' => Encodings::PLAIN,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::PLAIN, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_for_int32_column_defaults_to_dictionary(): void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default();
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::RLE_DICTIONARY, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_for_int32_column_with_writer_version_2(): void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::WRITER_VERSION, 2);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::DELTA_BINARY_PACKED, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_for_int64_column_with_writer_version_2(): void
    {
        $column = new FlatColumn('timestamp', PhysicalType::INT64);
        $options = Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::WRITER_VERSION, 2);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::DELTA_BINARY_PACKED, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_for_string_column_defaults_to_dictionary(): void
    {
        $column = new FlatColumn('name', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default();
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));

        static::assertContains(Encodings::RLE_DICTIONARY, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_with_custom_delta_binary_packed_encoding(): void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user_id' => Encodings::DELTA_BINARY_PACKED,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::DELTA_BINARY_PACKED, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_with_custom_plain_encoding(): void
    {
        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'description' => Encodings::PLAIN,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));

        static::assertContains(Encodings::PLAIN, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_with_custom_rle_dictionary_encoding(): void
    {
        $column = new FlatColumn('status', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'status' => Encodings::RLE_DICTIONARY,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_flat_path_encoding(): void
    {
        $column = new FlatColumn('user.id', PhysicalType::INT32);

        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user.id' => Encodings::DELTA_BINARY_PACKED,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::DELTA_BINARY_PACKED, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_with_rle_dictionary_encoding(): void
    {
        $column = new FlatColumn('status', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'status' => Encodings::RLE_DICTIONARY,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
    }

    public function test_empty_columns_encodings_option(): void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, []);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [7]));

        static::assertContains(Encodings::RLE_DICTIONARY, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_fallback_to_plain_when_no_custom_encoding_specified(): void
    {
        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::COLUMNS_ENCODINGS, [
            'other_column' => Encodings::RLE_DICTIONARY,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));

        static::assertContains(Encodings::PLAIN, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_invalid_delta_binary_packed_encoding_for_string_column_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. Column 'description' has type: BYTE_ARRAY",
        );

        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'description' => Encodings::DELTA_BINARY_PACKED,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );
    }

    public function test_invalid_rle_dictionary_encoding_for_fixed_len_byte_array_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1. Column 'fixed_data' has type: FIXED_LEN_BYTE_ARRAY",
        );

        $column = new FlatColumn('fixed_data', PhysicalType::FIXED_LEN_BYTE_ARRAY);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'fixed_data' => Encodings::RLE_DICTIONARY,
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );
    }

    public function test_non_enum_encoding_value_is_ignored(): void
    {
        $column = new FlatColumn('data', PhysicalType::BYTE_ARRAY);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'data' => 'INVALID_ENCODING', // Non-enum value should be ignored
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            $compressions,
            StatisticsOptions::fromOptions($options),
        );

        // Should fallback to default behavior since non-enum is ignored
        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));

        static::assertContains(Encodings::RLE_DICTIONARY, $builder->flush(0)[0]->columnChunk->encodings());
    }

    /**
     * @return array<string, array{FlatColumn, int, bool|float|int|string, Encodings}>
     */
    public static function default_encodings_provider(): array
    {
        return [
            'v2 string' => [FlatColumn::string('c'), 2, 'a', Encodings::DELTA_BYTE_ARRAY],
            'v2 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 2, 'abcd', Encodings::DELTA_BYTE_ARRAY],
            'v1 string' => [FlatColumn::string('c'), 1, 'a', Encodings::PLAIN],
            'v2 double' => [FlatColumn::double('c'), 2, 1.5, Encodings::PLAIN],
        ];
    }

    /**
     * @return array<string, array{int, list<Encodings>}>
     */
    public static function boolean_encodings_provider(): array
    {
        return ['v1' => [1, [Encodings::RLE, Encodings::PLAIN]], 'v2' => [2, [Encodings::RLE]]];
    }

    #[DataProvider('boolean_encodings_provider')]
    /**
     * @param list<Encodings> $encodings
     */
    public function test_a_boolean_column_gets_no_dictionary(int $writerVersion, array $encodings): void
    {
        $column = FlatColumn::boolean('flag');
        $options = Options::default()->set(Option::WRITER_VERSION, $writerVersion);
        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [true]));

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);
        static::assertEqualsCanonicalizing($encodings, array_values($builder->flush(0)[0]->columnChunk->encodings()));
    }

    /**
     * @return array<string, array{FlatColumn, int, bool|float|int|string, Encodings}>
     */
    public static function dictionary_overflow_provider(): array
    {
        return [
            'v2 string' => [FlatColumn::string('c'), 2, 'a', Encodings::DELTA_BYTE_ARRAY],
            'v2 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 2, 'abcd', Encodings::DELTA_BYTE_ARRAY],
            'v2 int32' => [FlatColumn::int32('c'), 2, 7, Encodings::DELTA_BINARY_PACKED],
            'v2 int64' => [FlatColumn::int64('c'), 2, 7, Encodings::DELTA_BINARY_PACKED],
            'v2 double' => [FlatColumn::double('c'), 2, 1.5, Encodings::PLAIN],
            'v1 string' => [FlatColumn::string('c'), 1, 'a', Encodings::PLAIN],
        ];
    }

    #[DataProvider('dictionary_overflow_provider')]
    public function test_a_dictionary_overflow_falls_back_to_the_writer_version_fallback(
        FlatColumn $column,
        int $writerVersion,
        bool|float|int|string $value,
        Encodings $fallback,
    ): void {
        $options = Options::default()->set(Option::WRITER_VERSION, $writerVersion)->set(
            Option::DICTIONARY_PAGE_SIZE,
            1,
        );
        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        $encodings = $builder->flush(0)[0]->columnChunk->encodings();

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
        static::assertContains($fallback, $encodings);
        static::assertNotContains(Encodings::RLE_DICTIONARY, $encodings);
    }

    #[DataProvider('default_encodings_provider')]
    public function test_a_column_without_columns_encodings_writes_the_writer_version_fallback(
        FlatColumn $column,
        int $writerVersion,
        bool|float|int|string $value,
        Encodings $encoding,
    ): void {
        $options = Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(
            Option::WRITER_VERSION,
            $writerVersion,
        );
        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));

        static::assertInstanceOf(FlatColumnChunkBuilder::class, $builder);
        static::assertContains($encoding, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_create_builder_with_custom_delta_length_byte_array_encoding(): void
    {
        $column = FlatColumn::string('description');
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'description' => Encodings::DELTA_LENGTH_BYTE_ARRAY,
        ]);
        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], ['a']));

        static::assertContains(Encodings::DELTA_LENGTH_BYTE_ARRAY, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_invalid_delta_byte_array_encoding_for_int64_column_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns. '
            . "Column 'id' has type: INT64",
        );

        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, ['id' => Encodings::DELTA_BYTE_ARRAY]);

        ColumnChunkBuilderFactory::createBuilder(
            FlatColumn::int64('id'),
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
    }

    public function test_invalid_delta_length_byte_array_encoding_for_fixed_len_byte_array_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY columns. Column 'f' has type: FIXED_LEN_BYTE_ARRAY",
        );

        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, ['f' => Encodings::DELTA_LENGTH_BYTE_ARRAY]);

        ColumnChunkBuilderFactory::createBuilder(
            FlatColumn::fixedSizeByteArray('f', 4),
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
    }

    /**
     * @return array<string, array{FlatColumn, int, bool|float|int|string, Encodings}>
     */
    public static function dictionary_defaults_provider(): array
    {
        return [
            'v1 int64' => [FlatColumn::int64('c'), 1, 7, Encodings::RLE_DICTIONARY],
            'v2 int64' => [FlatColumn::int64('c'), 2, 7, Encodings::RLE_DICTIONARY],
            'v1 double' => [FlatColumn::double('c'), 1, 1.5, Encodings::RLE_DICTIONARY],
            'v2 string' => [FlatColumn::string('c'), 2, 'a', Encodings::RLE_DICTIONARY],
            'v1 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 1, 'abcd', Encodings::PLAIN],
            'v2 fixedSizeByteArray' => [FlatColumn::fixedSizeByteArray('c', 4), 2, 'abcd', Encodings::RLE_DICTIONARY],
        ];
    }

    #[DataProvider('dictionary_defaults_provider')]
    public function test_a_column_without_columns_encodings_is_dictionary_encoded_where_arrow_rs_does(
        FlatColumn $column,
        int $writerVersion,
        bool|float|int|string $value,
        Encodings $encoding,
    ): void {
        $options = Options::default()->set(Option::WRITER_VERSION, $writerVersion);
        $builder = ColumnChunkBuilderFactory::createBuilder(
            $column,
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));

        static::assertContains($encoding, $builder->flush(0)[0]->columnChunk->encodings());
    }

    public function test_a_columns_encodings_dictionary_entry_wins_over_a_disabled_dictionary(): void
    {
        $options = Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::COLUMNS_ENCODINGS, [
            'status' => Encodings::RLE_DICTIONARY,
        ]);

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, ColumnChunkBuilderFactory::createBuilder(
            FlatColumn::string('status'),
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        ));
    }

    public function test_create_builder_with_rle_dictionary_encoding_for_fixed_len_byte_array_under_writer_version_2(): void
    {
        $options = Options::default()->set(Option::WRITER_VERSION, 2)->set(Option::COLUMNS_ENCODINGS, [
            'f' => Encodings::RLE_DICTIONARY,
        ]);

        static::assertInstanceOf(RLEDictionaryChunkBuilder::class, ColumnChunkBuilderFactory::createBuilder(
            FlatColumn::fixedSizeByteArray('f', 4),
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        ));
    }

    public function test_invalid_rle_encoding_for_int32_column_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "RLE encoding is only supported for BOOLEAN columns. Column 'id' has type: INT32",
        );

        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, ['id' => Encodings::RLE]);

        ColumnChunkBuilderFactory::createBuilder(
            FlatColumn::int32('id'),
            $options,
            Compressions::UNCOMPRESSED,
            StatisticsOptions::fromOptions($options),
        );
    }
}
