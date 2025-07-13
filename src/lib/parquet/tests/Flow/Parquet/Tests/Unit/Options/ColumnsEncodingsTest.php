<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Options;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options\ColumnsEncodings;
use Flow\Parquet\ParquetFile\Encodings;
use PHPUnit\Framework\TestCase;

final class ColumnsEncodingsTest extends TestCase
{
    public function test_accepts_case_insensitive_encoding_names() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'col1' => 'plain',
            'col2' => 'rle_dictionary',
            'col3' => 'DELTA_BINARY_PACKED',
            'col4' => 'Plain',
            'col5' => 'RLE_Dictionary',
        ]);

        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('col1'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('col2'));
        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('col3'));
        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('col4'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('col5'));
    }

    public function test_accepts_encodings_enum_directly() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'user_id' => Encodings::DELTA_BINARY_PACKED,
            'status' => Encodings::RLE_DICTIONARY,
            'description' => Encodings::PLAIN,
        ]);

        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('user_id'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('status'));
        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('description'));
    }

    public function test_checks_if_flat_path_exists() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'user_id' => 'DELTA_BINARY_PACKED',
            'user.profile.name' => 'RLE_DICTIONARY',
        ]);

        self::assertTrue($columnsEncodings->hasFlatPath('user_id'));
        self::assertTrue($columnsEncodings->hasFlatPath('user.profile.name'));
        self::assertFalse($columnsEncodings->hasFlatPath('non_existing'));
        self::assertFalse($columnsEncodings->hasFlatPath(''));
    }

    public function test_complex_nested_flat_paths() : void
    {
        $input = [
            'struct_nested.struct_flat.list_of_ints.list.element' => 'DELTA_BINARY_PACKED',
            'struct_nested.struct_flat.map_of_string_int.key_value.key' => 'RLE_DICTIONARY',
            'struct_nested.struct_flat.map_of_string_int.key_value.value' => 'DELTA_BINARY_PACKED',
            'struct_deeply_nested.struct_0.struct_1.struct_2.struct_3.struct_4.string' => 'RLE_DICTIONARY',
        ];

        $columnsEncodings = ColumnsEncodings::fromArray($input);

        self::assertSame(4, $columnsEncodings->count());
        self::assertSame(
            Encodings::DELTA_BINARY_PACKED,
            $columnsEncodings->getEncodingForFlatPath('struct_nested.struct_flat.list_of_ints.list.element')
        );
        self::assertSame(
            Encodings::RLE_DICTIONARY,
            $columnsEncodings->getEncodingForFlatPath('struct_nested.struct_flat.map_of_string_int.key_value.key')
        );
        self::assertSame(
            Encodings::DELTA_BINARY_PACKED,
            $columnsEncodings->getEncodingForFlatPath('struct_nested.struct_flat.map_of_string_int.key_value.value')
        );
        self::assertSame(
            Encodings::RLE_DICTIONARY,
            $columnsEncodings->getEncodingForFlatPath('struct_deeply_nested.struct_0.struct_1.struct_2.struct_3.struct_4.string')
        );
    }

    public function test_create_method_throws_exception_for_non_enum() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Encoding must be an Encodings enum, got: string');

        ColumnsEncodings::create([
            'user_id' => 'DELTA_BINARY_PACKED',
        ]);
    }

    public function test_create_method_with_encodings_enum() : void
    {
        $columnsEncodings = ColumnsEncodings::create([
            'user_id' => Encodings::DELTA_BINARY_PACKED,
            'status' => Encodings::RLE_DICTIONARY,
            'description' => Encodings::PLAIN,
        ]);

        self::assertSame(3, $columnsEncodings->count());
        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('user_id'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('status'));
        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('description'));
    }

    public function test_creates_columns_encodings_from_valid_array() : void
    {
        $input = [
            'user_id' => 'DELTA_BINARY_PACKED',
            'status' => 'RLE_DICTIONARY',
            'description' => 'PLAIN',
        ];

        $columnsEncodings = ColumnsEncodings::fromArray($input);

        self::assertFalse($columnsEncodings->isEmpty());
        self::assertSame(3, $columnsEncodings->count());
        self::assertSame(['user_id', 'status', 'description'], $columnsEncodings->getFlatPaths());
        self::assertSame($input, $columnsEncodings->toArray());
    }

    public function test_creates_columns_encodings_with_nested_flat_paths() : void
    {
        $input = [
            'user.id' => 'DELTA_BINARY_PACKED',
            'user.profile.name' => 'RLE_DICTIONARY',
            'orders.list.element' => 'DELTA_BINARY_PACKED',
            'metadata.key_value.key' => 'RLE_DICTIONARY',
            'metadata.key_value.value' => 'PLAIN',
        ];

        $columnsEncodings = ColumnsEncodings::fromArray($input);

        self::assertSame(5, $columnsEncodings->count());
        self::assertSame(
            ['user.id', 'user.profile.name', 'orders.list.element', 'metadata.key_value.key', 'metadata.key_value.value'],
            $columnsEncodings->getFlatPaths()
        );
        self::assertSame($input, $columnsEncodings->toArray());
    }

    public function test_creates_empty_columns_encodings_from_empty_array() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([]);

        self::assertTrue($columnsEncodings->isEmpty());
        self::assertSame(0, $columnsEncodings->count());
        self::assertSame([], $columnsEncodings->getFlatPaths());
        self::assertSame([], $columnsEncodings->toArray());
    }

    public function test_gets_encoding_for_existing_flat_path() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'user_id' => 'DELTA_BINARY_PACKED',
            'status' => 'RLE_DICTIONARY',
        ]);

        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('user_id'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('status'));
    }

    public function test_handles_whitespace_in_encoding_names() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'col1' => '  PLAIN  ',
            'col2' => "\tRLE_DICTIONARY\n",
            'col3' => ' DELTA_BINARY_PACKED ',
        ]);

        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('col1'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('col2'));
        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('col3'));
    }

    public function test_mixed_enum_and_string_input() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'user_id' => Encodings::DELTA_BINARY_PACKED,
            'status' => 'RLE_DICTIONARY',
            'description' => Encodings::PLAIN,
        ]);

        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings->getEncodingForFlatPath('user_id'));
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings->getEncodingForFlatPath('status'));
        self::assertSame(Encodings::PLAIN, $columnsEncodings->getEncodingForFlatPath('description'));
    }

    public function test_preserves_flat_path_order() : void
    {
        $input = [
            'z_column' => 'PLAIN',
            'a_column' => 'RLE_DICTIONARY',
            'm_column' => 'DELTA_BINARY_PACKED',
        ];

        $columnsEncodings = ColumnsEncodings::fromArray($input);

        self::assertSame(['z_column', 'a_column', 'm_column'], $columnsEncodings->getFlatPaths());
        self::assertSame($input, $columnsEncodings->toArray());
    }

    public function test_returns_null_for_non_existing_flat_path() : void
    {
        $columnsEncodings = ColumnsEncodings::fromArray([
            'user_id' => 'DELTA_BINARY_PACKED',
        ]);

        self::assertNull($columnsEncodings->getEncodingForFlatPath('non_existing'));
        self::assertNull($columnsEncodings->getEncodingForFlatPath(''));
    }

    public function test_throws_exception_for_empty_flat_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column flat path cannot be empty');

        ColumnsEncodings::fromArray([
            '' => 'PLAIN',
        ]);
    }

    public function test_throws_exception_for_invalid_encoding_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Unsupported encoding: 'INVALID_ENCODING'. Supported encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED");

        ColumnsEncodings::fromArray([
            'user_id' => 'INVALID_ENCODING',
        ]);
    }

    public function test_throws_exception_for_invalid_encoding_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Encoding must be an Encodings enum or string, got: integer');

        ColumnsEncodings::fromArray([
            'user_id' => 123,
        ]);
    }

    public function test_throws_exception_for_non_string_flat_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column flat path must be a string, got: integer');

        ColumnsEncodings::fromArray([
            123 => 'PLAIN',
        ]);
    }

    public function test_to_array_returns_encoding_names() : void
    {
        $input = [
            'user_id' => 'DELTA_BINARY_PACKED',
            'status' => 'RLE_DICTIONARY',
            'description' => 'PLAIN',
        ];

        $columnsEncodings = ColumnsEncodings::fromArray($input);
        $result = $columnsEncodings->toArray();

        self::assertSame($input, $result);
    }
}
