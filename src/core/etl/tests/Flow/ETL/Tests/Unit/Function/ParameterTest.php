<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, lit, ref, row, str_entry};
use function Flow\Types\DSL\{type_boolean, type_integer, type_null, type_string};
use Flow\Doctrine\Bulk\SQLParametersStyle;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\{BooleanType, IntegerType, StringType};
use PHPUnit\Framework\Attributes\DataProvider;

final class ParameterTest extends FlowTestCase
{
    public static function boolean_data_provider() : \Generator
    {
        yield 'string true' => ['true', true];
        yield 'string false' => ['false', true];
        yield 'string empty' => ['', false];
        yield 'string zero' => ['0', false];
        yield 'string one' => ['1', true];
        yield 'integer zero' => [0, false];
        yield 'integer one' => [1, true];
        yield 'integer negative' => [-1, true];
        yield 'float zero' => [0.0, false];
        yield 'float positive' => [1.5, true];
        yield 'boolean true' => [true, true];
        yield 'boolean false' => [false, false];
        yield 'array non-scalar' => [['value'], false];
        yield 'object non-scalar' => [new \stdClass(), false];
        yield 'null non-scalar' => [null, false];
    }

    public static function float_data_provider() : \Generator
    {
        yield 'valid float' => [3.14, 3.14];
        yield 'zero float' => [0.0, 0.0];
        yield 'negative float' => [-2.5, -2.5];
        yield 'integer not float' => [42, null];
        yield 'string not float' => ['3.14', null];
        yield 'boolean not float' => [true, null];
        yield 'array not float' => [[], null];
        yield 'null not float' => [null, null];
    }

    public static function int_data_provider() : \Generator
    {
        yield 'valid integer' => [42, null, 42];
        yield 'zero integer' => [0, null, 0];
        yield 'negative integer' => [-123, null, -123];
        yield 'float not integer with default' => [3.14, 99, 99];
        yield 'string not integer with default' => ['42', 99, 99];
        yield 'boolean not integer with default' => [true, 99, 99];
        yield 'null not integer with default' => [null, 99, 99];
        yield 'float not integer without default' => [3.14, null, null];
    }

    public static function number_data_provider() : \Generator
    {
        yield 'integer' => [42, null, 42];
        yield 'float' => [3.14, null, 3.14];
        yield 'zero' => [0, null, 0];
        yield 'negative integer' => [-42, null, -42];
        yield 'negative float' => [-3.14, null, -3.14];
        yield 'string with default' => ['not numeric', 99, 99];
        yield 'string with float default' => ['not numeric', 99.5, 99.5];
        yield 'boolean with default' => [true, 99, 99];
        yield 'array with default' => [[], 99, 99];
        yield 'null with default' => [null, 99, 99];
        yield 'string without default' => ['not numeric', null, null];
    }

    public static function string_data_provider() : \Generator
    {
        yield 'valid string' => ['hello', null, 'hello'];
        yield 'empty string' => ['', null, ''];
        yield 'numeric string' => ['123', null, '123'];
        yield 'integer with default' => [42, 'default', 'default'];
        yield 'float with default' => [3.14, 'default', 'default'];
        yield 'boolean with default' => [true, 'default', 'default'];
        yield 'array with default' => [[], 'default', 'default'];
        yield 'null with default' => [null, 'default', 'default'];
        yield 'integer without default' => [42, null, null];
    }

    public function test_as_array_with_empty_array() : void
    {
        $parameter = new Parameter(lit([]));
        self::assertSame([], $parameter->asArray(row()));
    }

    public function test_as_array_with_non_array() : void
    {
        $parameter = new Parameter(lit('not an array'));
        self::assertNull($parameter->asArray(row()));

        $parameter = new Parameter(lit(42));
        self::assertNull($parameter->asArray(row()));

        $parameter = new Parameter(lit(true));
        self::assertNull($parameter->asArray(row()));
    }

    public function test_as_array_with_valid_array() : void
    {
        $parameter = new Parameter(lit(['key' => 'value', 'number' => 42]));
        $result = $parameter->asArray(row());

        self::assertSame(['key' => 'value', 'number' => 42], $result);
    }

    #[DataProvider('boolean_data_provider')]
    public function test_as_boolean(mixed $input, bool $expected) : void
    {
        $parameter = new Parameter(lit($input));
        self::assertSame($expected, $parameter->asBoolean(row()));
    }

    public function test_as_entry_with_literal() : void
    {
        $parameter = new Parameter(lit('literal_value'));
        self::assertNull($parameter->asEntry(row()));
    }

    public function test_as_entry_with_missing_reference() : void
    {
        $parameter = new Parameter(ref('missing_column'));
        $row = row(str_entry('other_column', 'test_value'));

        self::assertNull($parameter->asEntry($row));
    }

    public function test_as_entry_with_reference() : void
    {
        $parameter = new Parameter(ref('test_column'));
        $row = row(str_entry('test_column', 'test_value'));

        $entry = $parameter->asEntry($row);
        self::assertNotNull($entry);
        self::assertSame('test_value', $entry->value());
    }

    public function test_as_enum_with_invalid_type() : void
    {
        $parameter = new Parameter(lit('not an enum'));
        self::assertNull($parameter->asEnum(row(), SQLParametersStyle::class));

        $parameter = new Parameter(lit(42));
        self::assertNull($parameter->asEnum(row(), SQLParametersStyle::class));
    }

    public function test_as_enum_with_valid_enum() : void
    {
        $parameter = new Parameter(lit(SQLParametersStyle::NAMED));
        $result = $parameter->asEnum(row(), SQLParametersStyle::class);

        self::assertSame(SQLParametersStyle::NAMED, $result);
    }

    public function test_as_enum_with_wrong_enum_class() : void
    {
        $parameter = new Parameter(lit(SQLParametersStyle::NAMED));
        self::assertNull($parameter->asEnum(row(), StringStyles::class));
    }

    #[DataProvider('float_data_provider')]
    public function test_as_float(mixed $input, ?float $expected) : void
    {
        $parameter = new Parameter(lit($input));
        self::assertSame($expected, $parameter->asFloat(row()));
    }

    public function test_as_instance_of_with_invalid_type() : void
    {
        $parameter = new Parameter(lit('not an object'));
        self::assertNull($parameter->asInstanceOf(row(), \DateTimeImmutable::class));

        $dateTime = new \DateTimeImmutable('2023-01-01');
        $parameter = new Parameter(lit($dateTime));
        self::assertNull($parameter->asInstanceOf(row(), \stdClass::class));
    }

    public function test_as_instance_of_with_valid_object() : void
    {
        $dateTime = new \DateTimeImmutable('2023-01-01');
        $parameter = new Parameter(lit($dateTime));

        $result = $parameter->asInstanceOf(row(), \DateTimeImmutable::class);
        self::assertSame($dateTime, $result);

        $result = $parameter->asInstanceOf(row(), \DateTimeInterface::class);
        self::assertSame($dateTime, $result);
    }

    #[DataProvider('int_data_provider')]
    public function test_as_int(mixed $input, ?int $default, ?int $expected) : void
    {
        $parameter = new Parameter(lit($input));
        self::assertSame($expected, $parameter->asInt(row(), $default));
    }

    public function test_as_list_of_objects_with_empty_array() : void
    {
        $parameter = new Parameter(lit([]));
        $result = $parameter->asListOfObjects(row(), \DateTimeImmutable::class);

        self::assertSame([], $result);
    }

    public function test_as_list_of_objects_with_mixed_types() : void
    {
        $date = new \DateTimeImmutable('2023-01-01');
        $parameter = new Parameter(lit([$date, 'not an object']));

        self::assertNull($parameter->asListOfObjects(row(), \DateTimeImmutable::class));
    }

    public function test_as_list_of_objects_with_non_array() : void
    {
        $parameter = new Parameter(lit('not an array'));
        self::assertNull($parameter->asListOfObjects(row(), \DateTimeImmutable::class));
    }

    public function test_as_list_of_objects_with_valid_array() : void
    {
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $parameter = new Parameter(lit([$date1, $date2]));

        $result = $parameter->asListOfObjects(row(), \DateTimeImmutable::class);
        self::assertSame([$date1, $date2], $result);
    }

    public function test_as_list_of_objects_with_wrong_object_type() : void
    {
        $date = new \DateTimeImmutable('2023-01-01');
        $std = new \stdClass();
        $parameter = new Parameter(lit([$date, $std]));

        self::assertNull($parameter->asListOfObjects(row(), \DateTimeImmutable::class));
    }

    #[DataProvider('number_data_provider')]
    public function test_as_number(mixed $input, int|float|null $default, int|float|null $expected) : void
    {
        $parameter = new Parameter(lit($input));
        self::assertSame($expected, $parameter->asNumber(row(), $default));
    }

    public function test_as_object_with_non_object() : void
    {
        $parameter = new Parameter(lit('not an object'));
        self::assertNull($parameter->asObject(row()));

        $parameter = new Parameter(lit(42));
        self::assertNull($parameter->asObject(row()));

        $parameter = new Parameter(lit([]));
        self::assertNull($parameter->asObject(row()));
    }

    public function test_as_object_with_valid_object() : void
    {
        $object = new \stdClass();
        $object->property = 'value';
        $parameter = new Parameter(lit($object));

        self::assertSame($object, $parameter->asObject(row()));
    }

    public function test_as_one_of() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_integer(), type_boolean()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string(), type_integer()));
    }

    public function test_as_one_of_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('42')));

        self::assertSame('42', $parameter->as(row(), type_string(), type_integer()));
        self::assertNull($parameter->as(row(), type_boolean()));
    }

    public function test_as_scalar() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_integer()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string()));
    }

    public function test_as_scalar_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));

        self::assertNull($parameter->as(row(), type_integer()));
        self::assertSame('test', $parameter->as(row(), type_string()));
    }

    #[DataProvider('string_data_provider')]
    public function test_as_string(mixed $input, ?string $default, ?string $expected) : void
    {
        $parameter = new Parameter(lit($input));
        self::assertSame($expected, $parameter->asString(row(), $default));
    }

    public function test_as_type_when_handling_string_entry_created_from_null() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertEquals(type_null(), $parameter->asType(row(StringEntry::fromNull('value'))));
    }

    public function test_as_type_with_literal_value() : void
    {
        $parameter = new Parameter(lit('string_value'));
        self::assertInstanceOf(StringType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(42));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(true));
        self::assertInstanceOf(BooleanType::class, $parameter->asType(row()));
    }

    public function test_as_type_with_reference() : void
    {
        $parameter = new Parameter(ref('value'));
        self::assertInstanceOf(StringType::class, $parameter->asType(row(str_entry('value', 'test'))));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row(int_entry('value', 42))));
    }

    public function test_as_type_with_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));
        self::assertInstanceOf(StringType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(ScalarResult::from(123)));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row()));
    }

    public function test_constructor_with_mixed_value() : void
    {
        $parameter = new Parameter('direct_string');
        self::assertSame('direct_string', $parameter->eval(row()));

        $parameter = new Parameter(123);
        self::assertSame(123, $parameter->eval(row()));

        $parameter = new Parameter(true);
        self::assertTrue($parameter->eval(row()));
    }

    public function test_constructor_with_scalar_function() : void
    {
        $scalarFunction = lit('test');
        $parameter = new Parameter($scalarFunction);

        self::assertSame('test', $parameter->eval(row()));
    }

    public function test_eval_with_direct_value() : void
    {
        $parameter = new Parameter(lit('direct_value'));
        self::assertSame('direct_value', $parameter->eval(row()));

        $parameter = new Parameter(lit(42));
        self::assertSame(42, $parameter->eval(row()));
    }

    public function test_eval_with_reference() : void
    {
        $parameter = new Parameter(ref('column'));
        $result = $parameter->eval(row(str_entry('column', 'ref_value')));

        self::assertSame('ref_value', $result);
    }

    public function test_eval_with_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test_value')));
        self::assertSame('test_value', $parameter->eval(row()));
    }
}
