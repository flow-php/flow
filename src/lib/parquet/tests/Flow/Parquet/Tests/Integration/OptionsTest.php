<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\Writer\StatisticsLevel;
use PHPUnit\Framework\TestCase;

use function count;

final class OptionsTest extends TestCase
{
    public function test_columns_compressions_integration_with_compressions_enum(): void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, [
            'user_id' => Compressions::SNAPPY,
            'description' => Compressions::GZIP,
            'metadata' => Compressions::ZSTD,
        ]);

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);
        static::assertIsArray($columnsCompressions);
        static::assertSame(Compressions::SNAPPY, $columnsCompressions['user_id']);
        static::assertSame(Compressions::GZIP, $columnsCompressions['description']);
        static::assertSame(Compressions::ZSTD, $columnsCompressions['metadata']);
    }

    public function test_columns_encodings_integration_with_encodings_enum(): void
    {
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, [
            'user_id' => Encodings::DELTA_BINARY_PACKED,
            'status' => Encodings::RLE_DICTIONARY,
            'description' => Encodings::PLAIN,
        ]);

        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        static::assertIsArray($columnsEncodings);
        static::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings['user_id']);
        static::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings['status']);
        static::assertSame(Encodings::PLAIN, $columnsEncodings['description']);
    }

    public function test_complex_scenario_with_both_compressions_and_encodings(): void
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
        static::assertIsArray($columnsCompressions);
        static::assertSame(6, count($columnsCompressions));
        static::assertSame(Compressions::SNAPPY, $columnsCompressions['user.id']);
        static::assertSame(Compressions::LZ4, $columnsCompressions['orders.list.element.id']);
        static::assertSame(Compressions::UNCOMPRESSED, $columnsCompressions['metadata.key_value.value']);

        // Verify encodings
        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        static::assertIsArray($columnsEncodings);
        static::assertSame(6, count($columnsEncodings));
        static::assertSame(Encodings::DELTA_BINARY_PACKED, $columnsEncodings['user.id']);
        static::assertSame(Encodings::RLE_DICTIONARY, $columnsEncodings['user.profile.name']);
        static::assertSame(Encodings::PLAIN, $columnsEncodings['metadata.key_value.value']);

        // Verify other options
        static::assertTrue($options->getBool(Option::VALIDATE_DATA));
        static::assertSame(2000, $options->getInt(Option::PAGE_MAXIMUM_ROWS_COUNT));
        static::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_default_options_have_null_columns_compressions(): void
    {
        $options = Options::default();

        static::assertNull($options->getArray(Option::COLUMNS_COMPRESSIONS));
        static::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
    }

    public function test_default_options_have_null_columns_encodings(): void
    {
        $options = Options::default();

        static::assertNull($options->getArray(Option::COLUMNS_ENCODINGS));
        static::assertFalse($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_get_method_returns_correct_type_for_columns_compressions(): void
    {
        $compressionArray = ['user_id' => Compressions::SNAPPY];
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, $compressionArray);

        $value = $options->get(Option::COLUMNS_COMPRESSIONS);
        static::assertIsArray($value);
        static::assertSame(Compressions::SNAPPY, $value['user_id']);
    }

    public function test_get_method_returns_correct_type_for_columns_encodings(): void
    {
        $encodingArray = ['user_id' => Encodings::PLAIN];
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, $encodingArray);

        $value = $options->get(Option::COLUMNS_ENCODINGS);
        static::assertIsArray($value);
        static::assertSame(Encodings::PLAIN, $value['user_id']);
    }

    public function test_get_method_returns_null_for_unset_columns_options(): void
    {
        $options = Options::default();

        static::assertNull($options->get(Option::COLUMNS_COMPRESSIONS));
        static::assertNull($options->get(Option::COLUMNS_ENCODINGS));
    }

    public function test_has_method_correctly_identifies_set_options(): void
    {
        $options = Options::default();

        // Initially both should be false
        static::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertFalse($options->has(Option::COLUMNS_ENCODINGS));

        // Set compressions
        $options->set(Option::COLUMNS_COMPRESSIONS, ['user_id' => Compressions::SNAPPY]);
        static::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertFalse($options->has(Option::COLUMNS_ENCODINGS));

        // Set encodings
        $options->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN]);
        static::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertTrue($options->has(Option::COLUMNS_ENCODINGS));

        // Set to null
        $options->set(Option::COLUMNS_COMPRESSIONS, null);
        static::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_options_are_fluent(): void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, [
            'user_id' => Compressions::SNAPPY,
        ])->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN])->set(Option::VALIDATE_DATA, false);

        static::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));
        static::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
        static::assertFalse($options->getBool(Option::VALIDATE_DATA));
    }

    public function test_set_columns_compressions_to_null(): void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, ['user_id' => Compressions::SNAPPY])->set(
            Option::COLUMNS_COMPRESSIONS,
            null,
        );

        static::assertNull($options->getArray(Option::COLUMNS_COMPRESSIONS));
        static::assertFalse($options->has(Option::COLUMNS_COMPRESSIONS));
    }

    public function test_set_columns_encodings_to_null(): void
    {
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, ['user_id' => Encodings::PLAIN])->set(
            Option::COLUMNS_ENCODINGS,
            null,
        );

        static::assertNull($options->getArray(Option::COLUMNS_ENCODINGS));
        static::assertFalse($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_set_empty_arrays(): void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, [])->set(Option::COLUMNS_ENCODINGS, []);

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);
        static::assertIsArray($columnsCompressions);
        static::assertSame(0, count($columnsCompressions));
        static::assertTrue($options->has(Option::COLUMNS_COMPRESSIONS));

        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);
        static::assertIsArray($columnsEncodings);
        static::assertSame(0, count($columnsEncodings));
        static::assertTrue($options->has(Option::COLUMNS_ENCODINGS));
    }

    public function test_an_enum_option_is_read_as_its_enum_class(): void
    {
        static::assertSame(StatisticsLevel::PAGE, (new Options())->getEnum(
            Option::STATISTICS_LEVEL,
            StatisticsLevel::class,
        ));
        static::assertSame(StatisticsLevel::NONE, (new Options())->set(
            Option::STATISTICS_LEVEL,
            StatisticsLevel::NONE,
        )->getEnum(Option::STATISTICS_LEVEL, StatisticsLevel::class));
    }

    public function test_an_enum_option_of_another_class_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Option STATISTICS_LEVEL is not a Flow\\Parquet\\Writer\\StatisticsLevel, but: Flow\\Parquet\\ParquetFile\\Compressions',
        );

        (new Options())->set(Option::STATISTICS_LEVEL, Compressions::GZIP)->getEnum(
            Option::STATISTICS_LEVEL,
            StatisticsLevel::class,
        );
    }

    public function test_normalize_round_trips_every_default_option(): void
    {
        $options = Options::default();

        static::assertEquals($options, Options::fromArray($options->normalize()));
        static::assertSame(
            ['enum' => StatisticsLevel::class, 'case' => 'PAGE'],
            $options->normalize()['STATISTICS_LEVEL'],
        );
    }

    public function test_normalize_round_trips_enum_maps_and_bloom_filters(): void
    {
        $options = Options::default()->set(Option::COLUMNS_COMPRESSIONS, [
            'a' => Compressions::SNAPPY,
        ])->set(Option::COLUMNS_ENCODINGS, [
            'a' => Encodings::PLAIN,
        ])->set(Option::COLUMNS_BLOOM_FILTERS, ['a' => new BloomFilterOptions(0.1, 200)]);

        static::assertSame(
            ['a' => ['bloom_filter' => ['fpp' => 0.1, 'ndv' => 200]]],
            $options->normalize()['COLUMNS_BLOOM_FILTERS'],
        );
        static::assertEquals($options, Options::fromArray($options->normalize()));
    }

    public function test_from_array_refuses_an_unknown_option(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown parquet option: NOPE');

        Options::fromArray(['NOPE' => 1]);
    }

    public function test_from_array_refuses_a_value_naming_a_class_that_is_not_an_enum(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Option enum values must name an enum class and one of its cases');

        Options::fromArray(['STATISTICS_LEVEL' => ['enum' => Options::class, 'case' => 'PAGE']]);
    }

    public function test_from_array_refuses_a_bloom_filter_without_fpp(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A bloom filter option must hold a numeric "fpp"');

        Options::fromArray(['COLUMNS_BLOOM_FILTERS' => ['a' => ['bloom_filter' => ['ndv' => 1]]]]);
    }

    public function test_from_array_refuses_an_object_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Options::fromArray(['PAGE_SIZE_BYTES' => new BloomFilterOptions()]);
    }
}
