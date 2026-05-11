<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\RowsNormalizer;

use Flow\ETL\Adapter\Excel\RowsNormalizer\ExcelRowsNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

enum BackedStringTestEnum: string
{
    case ACTIVE = 'active';
    case INACTIVE = 'inactive';
}

enum BackedIntTestEnum: int
{
    case ONE = 1;
    case TWO = 2;
}

enum UnitTestEnum
{
    case COMPLETE;
    case PENDING;
}

final class ExcelRowsNormalizerTest extends FlowTestCase
{
    public function test_headers(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $headers = $normalizer->headers(row(
            int_entry('id', 1),
            string_entry('name', 'Test'),
            float_entry('value', 1.5),
        ));

        static::assertSame(['id', 'name', 'value'], $headers);
    }

    public function test_normalize_backed_int_enum(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(enum_entry('count', BackedIntTestEnum::TWO)));

        static::assertSame(['2'], $result);
    }

    public function test_normalize_backed_string_enum(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(enum_entry('status', BackedStringTestEnum::ACTIVE)));

        static::assertSame(['active'], $result);
    }

    public function test_normalize_json_entry_with_null_value(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(json_entry('data', null)));

        static::assertSame([null], $result);
    }

    public function test_normalize_json_entry_with_value(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(json_entry('data', ['key' => 'value'])));

        static::assertSame(['{"key":"value"}'], $result);
    }

    public function test_normalize_list_entry(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(list_entry('items', [1, 2, 3], type_list(type_integer()))));

        static::assertSame(['[1,2,3]'], $result);
    }

    public function test_normalize_map_entry(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(map_entry(
            'mapping',
            ['a' => 1, 'b' => 2],
            type_map(type_string(), type_integer()),
        )));

        static::assertSame(['{"a":1,"b":2}'], $result);
    }

    public function test_normalize_mixed_row(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(
            int_entry('id', 42),
            string_entry('name', 'Test'),
            float_entry('price', 19.99),
            enum_entry('status', BackedStringTestEnum::ACTIVE),
        ));

        static::assertSame([42, 'Test', 19.99, 'active'], $result);
    }

    public function test_normalize_null_enum_entry(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(enum_entry('status', null)));

        static::assertSame([null], $result);
    }

    public function test_normalize_structure_entry(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(structure_entry('person', ['name' => 'John', 'age' => 30], type_structure([
            'name' => type_string(),
            'age' => type_integer(),
        ]))));

        static::assertSame(['{"name":"John","age":30}'], $result);
    }

    public function test_normalize_unit_enum(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(enum_entry('status', UnitTestEnum::PENDING)));

        static::assertSame(['PENDING'], $result);
    }

    public function test_normalize_xml_entry(): void
    {
        $normalizer = new ExcelRowsNormalizer();

        $result = $normalizer->normalize(row(xml_entry('xml', '<root><item>value</item></root>')));

        static::assertSame(['<root><item>value</item></root>'], $result);
    }
}
