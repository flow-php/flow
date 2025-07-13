<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\{DeltaBinaryPackedColumnChunkBuilder, PlainFlatColumnChunkBuilder, RLEDictionaryChunkBuilder};
use Flow\Parquet\Writer\ColumnChunkBuilderFactory;
use PHPUnit\Framework\TestCase;

final class ColumnChunkBuilderFactoryTest extends TestCase
{
    public function test_create_builder_custom_encoding_overrides_automatic_selection() : void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()
            ->set(Option::WRITER_VERSION, 2)
            ->set(Option::COLUMNS_ENCODINGS, [
                'user_id' => 'PLAIN',
            ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_for_int32_column_with_plain_encoding() : void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default();
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_for_int32_column_with_writer_version_2() : void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::WRITER_VERSION, 2);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_for_int64_column_with_writer_version_2() : void
    {
        $column = new FlatColumn('timestamp', PhysicalType::INT64);
        $options = Options::default()->set(Option::WRITER_VERSION, 2);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_for_string_column() : void
    {
        $column = new FlatColumn('name', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default();
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_case_insensitive_encoding_names() : void
    {
        $column = new FlatColumn('status', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'status' => 'rle_dictionary',  // lowercase
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_custom_delta_binary_packed_encoding() : void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user_id' => 'DELTA_BINARY_PACKED',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_custom_plain_encoding() : void
    {
        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'description' => 'PLAIN',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_custom_rle_dictionary_encoding() : void
    {
        $column = new FlatColumn('status', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'status' => 'RLE_DICTIONARY',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(RLEDictionaryChunkBuilder::class, $builder);
    }

    public function test_create_builder_with_flat_path_encoding() : void
    {
        $column = new FlatColumn('user.id', PhysicalType::INT32);

        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user.id' => 'DELTA_BINARY_PACKED',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    public function test_empty_columns_encodings_option() : void
    {
        $column = new FlatColumn('user_id', PhysicalType::INT32);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, []);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_fallback_to_plain_when_no_custom_encoding_specified() : void
    {
        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'other_column' => 'RLE_DICTIONARY',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);

        self::assertInstanceOf(PlainFlatColumnChunkBuilder::class, $builder);
    }

    public function test_invalid_delta_binary_packed_encoding_for_string_column_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. Column 'description' has type: BYTE_ARRAY");

        $column = new FlatColumn('description', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::string());
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'description' => 'DELTA_BINARY_PACKED',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);
    }

    public function test_invalid_rle_dictionary_encoding_for_fixed_len_byte_array_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns. Column 'fixed_data' has type: FIXED_LEN_BYTE_ARRAY");

        $column = new FlatColumn('fixed_data', PhysicalType::FIXED_LEN_BYTE_ARRAY);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'fixed_data' => 'RLE_DICTIONARY',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);
    }

    public function test_unsupported_encoding_name_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Unsupported encoding: 'INVALID_ENCODING'. Supported encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED");

        $column = new FlatColumn('data', PhysicalType::BYTE_ARRAY);
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'data' => 'INVALID_ENCODING',
        ]);
        $compressions = Compressions::UNCOMPRESSED;

        ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);
    }
}
