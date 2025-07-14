<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration;

use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions, Encodings};
use PHPUnit\Framework\TestCase;

final class OptionsTest extends TestCase
{
    public function test_columns_compressions_integration_with_compressions_enum() : void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, [
            'user_id' => Compressions::SNAPPY,
            'description' => Compressions::GZIP,
            'metadata' => Compressions::ZSTD,
        ]);

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);
        self::assertIsArray($columnsCompressions);
        self::assertSame(Compressions::SNAPPY, $columnsCompressions['user_id']);
        self::assertSame(Compressions::GZIP, $columnsCompressions['description']);
        self::assertSame(Compressions::ZSTD, $columnsCompressions['metadata']);
    }

    public function test_columns_encodings_integration_with_encodings_enum() : void
    {
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user_id' => Encodings::DELTA_BINARY_PACKED,
            'status' => Encodings::RLE_DICTIONARY,
            'description' => Encodings::PLAIN,
        ]);

        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        self::assertIsArray($columnsEncodings);
        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings['user_id']);
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings['status']);
        self::assertSame(Encodings::PLAIN, $columnsEncodings['description']);
    }

    public function test_complex_scenario_with_both_compressions_and_encodings() : void
    {
        $compressionConfig = [
            'user.id' => Compressions::SNAPPY,
            'user.profile.name' => Compressions::GZIP,
            'orders.list.element.id' => Compressions::LZ4,
            'orders.list.element.amount' => Compressions::ZSTD,
            'metadata.key_value.key' => Compressions::BROTLI,
            'metadata.key_value.value' => Compressions::UNCOMPRESSED,
        ];

        $encodingConfig = [
            'user.id' => Encodings::DELTA_BINARY_PACKED,
            'user.profile.name' => Encodings::RLE_DICTIONARY,
            'orders.list.element.id' => Encodings::DELTA_BINARY_PACKED,
            'orders.list.element.amount' => Encodings::PLAIN,
            'metadata.key_value.key' => Encodings::RLE_DICTIONARY,
            'metadata.key_value.value' => Encodings::PLAIN,
        ];

        $options = Options::default()
            ->set(Option::COLUMNS_COMPRESSIONS, $compressionConfig)
            ->set(Option::COLUMNS_ENCODINGS, $encodingConfig)
            ->set(Option::VALIDATE_DATA, true)
            ->set(Option::PAGE_MAXIMUM_ROWS_COUNT, 2000);

        // Verify compressions
        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);
        self::assertIsArray($columnsCompressions);
        self::assertSame(6, \count($columnsCompressions));
        self::assertSame(Compressions::SNAPPY, $columnsCompressions['user.id']);
        self::assertSame(Compressions::LZ4, $columnsCompressions['orders.list.element.id']);
        self::assertSame(Compressions::UNCOMPRESSED, $columnsCompressions['metadata.key_value.value']);

        // Verify encodings
        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        self::assertIsArray($columnsEncodings);
        self::assertSame(6, \count($columnsEncodings));
        self::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings['user.id']);
        self::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings['user.profile.name']);
        self::assertSame(Encodings::PLAIN, $columnsEncodings['metadata.key_value.value']);

        // Verify other options
        self::assertTrue($options->getBool(Option::VALIDATE_DATA));
        self::assertSame(2000, $options->getInt(Option::PAGE_MAXIMUM_ROWS_COUNT));
        self::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_default_options_have_null_columns_compressions() : void
    {
        $options = Options::default();

        self::assertNull($options->getArray(Option::COLUMNS_COMPRESSIONS));
        self::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
    }

    public function test_default_options_have_null_columns_encodings() : void
    {
        $options = Options::default();

        self::assertNull($options->getArray(Option::COLUMNS_ENCODINGS));
        self::assertFalse($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_get_method_returns_correct_type_for_columns_compressions() : void
    {
        $compressionArray = ['user_id' => Compressions::SNAPPY];
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, $compressionArray);

        $value = $options->get(Option::COLUMNS_COMPRESSIONS);
        self::assertIsArray($value);
        self::assertSame(Compressions::SNAPPY, $value['user_id']);
    }

    public function test_get_method_returns_correct_type_for_columns_encodings() : void
    {
        $encodingArray = ['user_id' => Encodings::PLAIN];
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, $encodingArray);

        $value = $options->get(Option::COLUMNS_ENCODINGS);
        self::assertIsArray($value);
        self::assertSame(Encodings::PLAIN, $value['user_id']);
    }

    public function test_get_method_returns_null_for_unset_columns_options() : void
    {
        $options = Options::default();

        self::assertNull($options->get(Option::COLUMNS_COMPRESSIONS));
        self::assertNull($options->get(Option::COLUMNS_ENCODINGS));
    }

    public function test_has_method_correctly_identifies_set_options() : void
    {
        $options = Options::default();

        // Initially both should be false
        self::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertFalse($options->has(Option::COLUMNS_ENCODINGS));

        // Set compressions
        $options->set(Option::COLUMNS_COMPRESSIONS, ['user_id' => Compressions::SNAPPY]);
        self::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertFalse($options->has(Option::COLUMNS_ENCODINGS));

        // Set encodings
        $options->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN]);
        self::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertTrue($options->has(Option::COLUMNS_ENCODINGS));

        // Set to null
        $options->set(Option::COLUMNS_COMPRESSIONS, null);
        self::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_options_are_fluent() : void
    {
        $options = Options::default()
            ->set(Option::COLUMNS_COMPRESSIONS, ['user_id' => Compressions::SNAPPY])
            ->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN])
            ->set(Option::VALIDATE_DATA, false);

        self::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        self::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
        self::assertFalse($options->getBool(Option::VALIDATE_DATA));
    }

    public function test_set_columns_compressions_to_null() : void
    {
        $options = Options::default()
            ->set(Option::COLUMNS_COMPRESSIONS, ['user_id' => Compressions::SNAPPY])
            ->set(Option::COLUMNS_COMPRESSIONS, null);

        self::assertNull($options->getArray(Option::COLUMNS_COMPRESSIONS));
        self::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
    }

    public function test_set_columns_encodings_to_null() : void
    {
        $options = Options::default()
            ->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN])
            ->set(Option::COLUMNS_ENCODINGS, null);

        self::assertNull($options->getArray(Option::COLUMNS_ENCODINGS));
        self::assertFalse($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_set_empty_arrays() : void
    {
        $options = Options::default()
            ->set(Option::COLUMNS_COMPRESSIONS, [])
            ->set(Option::COLUMNS_ENCODINGS, []);

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);
        self::assertIsArray($columnsCompressions);
        self::assertSame(0, \count($columnsCompressions));
        self::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));

        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        self::assertIsArray($columnsEncodings);
        self::assertSame(0, \count($columnsEncodings));
        self::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }
}
