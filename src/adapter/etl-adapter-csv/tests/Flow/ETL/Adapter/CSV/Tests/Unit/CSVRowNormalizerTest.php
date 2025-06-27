<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVRowNormalizer;
use PHPUnit\Framework\TestCase;

final class CSVRowNormalizerTest extends TestCase
{
    public function test_normalize_complex_scenario_with_expansion_truncation_and_empty_conversion() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', '', 'value3', '', 'extraValue'];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', null, 'value3', null], $result);
        self::assertCount(4, $result);
    }

    public function test_normalize_converts_empty_strings_to_null_when_enabled() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', '', 'value3', ''];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', null, 'value3', null], $result);
    }

    public function test_normalize_handles_whitespace_strings_when_empty_to_null_enabled() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', '   ', 'value3', "\t"];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', '   ', 'value3', "\t"], $result);
    }

    public function test_normalize_mixed_types_in_row_data() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['string', '', null, '0', 'false'];
        $headersCount = 5;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['string', null, null, '0', 'false'], $result);
    }

    public function test_normalize_preserves_array_keys_when_truncating() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = [0 => 'value1', 1 => 'value2', 2 => 'value3', 3 => 'value4'];
        $headersCount = 2;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame([0 => 'value1', 1 => 'value2'], $result);
        self::assertArrayHasKey(0, $result);
        self::assertArrayHasKey(1, $result);
        self::assertArrayNotHasKey(2, $result);
    }

    public function test_normalize_preserves_empty_strings_when_disabled() : void
    {
        $normalizer = new CSVRowNormalizer(false);
        $rowData = ['value1', '', 'value3', ''];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', '', 'value3', ''], $result);
    }

    public function test_normalize_preserves_null_values() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', null, 'value3'];
        $headersCount = 3;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', null, 'value3'], $result);
    }

    public function test_normalize_row_data_fewer_columns_than_headers_with_empty_to_empty_string() : void
    {
        $normalizer = new CSVRowNormalizer(false);
        $rowData = ['value1', 'value2'];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', 'value2', '', ''], $result);
        self::assertCount(4, $result);
    }

    public function test_normalize_row_data_fewer_columns_than_headers_with_empty_to_null() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', 'value2'];
        $headersCount = 4;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', 'value2', null, null], $result);
        self::assertCount(4, $result);
    }

    public function test_normalize_row_data_more_columns_than_headers() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', 'value2', 'value3', 'value4', 'value5'];
        $headersCount = 3;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', 'value2', 'value3'], $result);
        self::assertCount(3, $result);
    }

    public function test_normalize_row_data_same_columns_as_headers() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', 'value2', 'value3'];
        $headersCount = 3;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['value1', 'value2', 'value3'], $result);
        self::assertCount(3, $result);
    }

    public function test_normalize_with_empty_row_data() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = [];
        $headersCount = 3;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame([null, null, null], $result);
        self::assertCount(3, $result);
    }

    public function test_normalize_with_empty_row_data_and_empty_to_false() : void
    {
        $normalizer = new CSVRowNormalizer(false);
        $rowData = [];
        $headersCount = 3;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame(['', '', ''], $result);
        self::assertCount(3, $result);
    }

    public function test_normalize_with_zero_headers() : void
    {
        $normalizer = new CSVRowNormalizer(true);
        $rowData = ['value1', 'value2'];
        $headersCount = 0;

        $result = $normalizer->normalize($rowData, $headersCount);

        self::assertSame([], $result);
        self::assertCount(0, $result);
    }
}
