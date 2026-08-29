<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Doctrine\Bulk\SQLParametersStyle;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Parameter;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use stdClass;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class ParameterTest extends FlowTestCase
{
    public static function boolean_data_provider(): Generator
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
        yield 'null propagates' => [null, null];
    }

    public static function float_data_provider(): Generator
    {
        yield 'valid float' => [3.14, 3.14];
        yield 'zero float' => [0.0, 0.0];
        yield 'negative float' => [-2.5, -2.5];
        yield 'null propagates' => [null, null];
    }

    public static function int_data_provider(): Generator
    {
        yield 'valid integer' => [42, null, 42];
        yield 'zero integer' => [0, null, 0];
        yield 'negative integer' => [-123, null, -123];
        yield 'null with default' => [null, 99, 99];
        yield 'null without default' => [null, null, null];
    }

    public static function number_data_provider(): Generator
    {
        yield 'integer' => [42, null, 42];
        yield 'float' => [3.14, null, 3.14];
        yield 'zero' => [0, null, 0];
        yield 'negative integer' => [-42, null, -42];
        yield 'negative float' => [-3.14, null, -3.14];
        yield 'integer as string' => ['99', null, 99];
        yield 'float as string' => ['99.5', null, 99.5];
        yield 'negative integer as string' => ['-42', null, -42];
        yield 'negative float as string' => ['-3.14', null, -3.14];
        yield 'null with default' => [null, 99, 99];
        yield 'null without default' => [null, null, null];
    }

    public static function string_data_provider(): Generator
    {
        yield 'valid string' => ['hello', null, 'hello'];
        yield 'empty string' => ['', null, ''];
        yield 'numeric string' => ['123', null, '123'];
        yield 'null with default' => [null, 'default', 'default'];
        yield 'null without default' => [null, null, null];
    }

    public function test_as_array_with_empty_array(): void
    {
        $parameter = new Parameter(lit([]));
        static::assertSame([], $parameter->asArray(row(), flow_context()));
    }

    public function test_as_boolean_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(['value'])))->asBoolean(row(), flow_context());
    }

    public function test_as_boolean_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asBoolean(row(), flow_context()));
    }

    public function test_as_float_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('3.14')))->asFloat(row(), flow_context());
    }

    public function test_as_float_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asFloat(row(), flow_context()));
    }

    public function test_as_int_throws_on_a_malformed_value(): void
    {
        // A default never applies to a malformed value - only to a NULL input.
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(3.14)))->asInt(row(), flow_context(), 99);
    }

    public function test_as_int_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asInt(row(), flow_context()));
    }

    public function test_as_number_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not numeric')))->asNumber(row(), flow_context(), 99);
    }

    public function test_as_number_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asNumber(row(), flow_context()));
    }

    public function test_as_string_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(42)))->asString(row(), flow_context(), 'default');
    }

    public function test_as_string_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asString(row(), flow_context()));
    }

    public function test_as_array_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an array')))->asArray(row(), flow_context());
    }

    public function test_as_array_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asArray(row(), flow_context()));
    }

    public function test_as_array_with_valid_array(): void
    {
        $parameter = new Parameter(lit(['key' => 'value', 'number' => 42]));
        $result = $parameter->asArray(row(), flow_context());

        static::assertSame(['key' => 'value', 'number' => 42], $result);
    }

    #[DataProvider('boolean_data_provider')]
    public function test_as_boolean(mixed $input, ?bool $expected): void
    {
        $parameter = new Parameter(lit($input));
        static::assertSame($expected, $parameter->asBoolean(row(), flow_context()));
    }

    public function test_as_entry_with_literal(): void
    {
        $parameter = new Parameter(lit('literal_value'));
        static::assertNull($parameter->asEntry(row()));
    }

    public function test_as_entry_with_missing_reference(): void
    {
        $parameter = new Parameter(ref('missing_column'));
        $row = row(str_entry('other_column', 'test_value'));

        static::assertNull($parameter->asEntry($row));
    }

    public function test_as_entry_with_reference(): void
    {
        $parameter = new Parameter(ref('test_column'));
        $row = row(str_entry('test_column', 'test_value'));

        $entry = $parameter->asEntry($row);
        static::assertNotNull($entry);
        static::assertSame('test_value', $entry->value());
    }

    public function test_as_enum_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an enum')))->asEnum(row(), flow_context(), SQLParametersStyle::class);
    }

    public function test_as_enum_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asEnum(row(), flow_context(), SQLParametersStyle::class));
    }

    public function test_as_enum_with_valid_enum(): void
    {
        $parameter = new Parameter(lit(SQLParametersStyle::NAMED));
        $result = $parameter->asEnum(row(), flow_context(), SQLParametersStyle::class);

        static::assertSame(SQLParametersStyle::NAMED, $result);
    }

    public function test_as_enum_with_wrong_enum_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(SQLParametersStyle::NAMED)))->asEnum(row(), flow_context(), StringStyles::class);
    }

    #[DataProvider('float_data_provider')]
    public function test_as_float(mixed $input, ?float $expected): void
    {
        $parameter = new Parameter(lit($input));
        static::assertSame($expected, $parameter->asFloat(row(), flow_context()));
    }

    public function test_as_instance_of_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an object')))->asInstanceOf(row(), flow_context(), DateTimeImmutable::class);
    }

    public function test_as_instance_of_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asInstanceOf(row(), flow_context(), DateTimeImmutable::class));
    }

    public function test_as_instance_of_with_valid_object(): void
    {
        $dateTime = new DateTimeImmutable('2023-01-01');
        $parameter = new Parameter(lit($dateTime));

        $result = $parameter->asInstanceOf(row(), flow_context(), DateTimeImmutable::class);
        static::assertSame($dateTime, $result);

        $result = $parameter->asInstanceOf(row(), flow_context(), DateTimeInterface::class);
        static::assertSame($dateTime, $result);
    }

    #[DataProvider('int_data_provider')]
    public function test_as_int(mixed $input, ?int $default, ?int $expected): void
    {
        $parameter = new Parameter(lit($input));
        static::assertSame($expected, $parameter->asInt(row(), flow_context(), $default));
    }

    public function test_as_list_of_objects_with_empty_array(): void
    {
        $parameter = new Parameter(lit([]));
        $result = $parameter->asListOfObjects(row(), flow_context(), DateTimeImmutable::class);

        static::assertSame([], $result);
    }

    public function test_as_list_of_objects_throws_on_a_malformed_element(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit([new DateTimeImmutable('2023-01-01'), 'not an object'])))->asListOfObjects(
            row(),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    public function test_as_list_of_objects_throws_on_a_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an array')))->asListOfObjects(row(), flow_context(), DateTimeImmutable::class);
    }

    public function test_as_list_of_objects_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asListOfObjects(
            row(),
            flow_context(),
            DateTimeImmutable::class,
        ));
    }

    public function test_as_list_of_objects_with_valid_array(): void
    {
        $date1 = new DateTimeImmutable('2023-01-01');
        $date2 = new DateTimeImmutable('2023-01-02');
        $parameter = new Parameter(lit([$date1, $date2]));

        $result = $parameter->asListOfObjects(row(), flow_context(), DateTimeImmutable::class);
        static::assertSame([$date1, $date2], $result);
    }

    public function test_as_list_of_objects_with_wrong_object_type(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit([new DateTimeImmutable('2023-01-01'), new stdClass()])))->asListOfObjects(
            row(),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    #[DataProvider('number_data_provider')]
    public function test_as_number(mixed $input, int|float|null $default, int|float|null $expected): void
    {
        $parameter = new Parameter(lit($input));
        static::assertSame($expected, $parameter->asNumber(row(), flow_context(), $default));
    }

    public function test_as_object_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an object')))->asObject(row(), flow_context());
    }

    public function test_as_object_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->asObject(row(), flow_context()));
    }

    public function test_as_object_with_valid_object(): void
    {
        $object = new stdClass();
        $object->property = 'value';
        $parameter = new Parameter(lit($object));

        static::assertSame($object, $parameter->asObject(row(), flow_context()));
    }

    public function test_as_one_of(): void
    {
        static::assertSame('42', (new Parameter(ref('value')))->as(
            row(str_entry('value', '42')),
            flow_context(),
            type_string(),
            type_integer(),
        ));
    }

    public function test_as_throws_when_no_type_matches(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(ref('value')))->as(
            row(str_entry('value', '42')),
            flow_context(),
            type_integer(),
            type_boolean(),
        );
    }

    public function test_as_propagates_null(): void
    {
        static::assertNull((new Parameter(lit(null)))->as(row(), flow_context(), type_integer()));
    }

    public function test_as_scalar(): void
    {
        static::assertSame('42', (new Parameter(ref('value')))->as(
            row(str_entry('value', '42')),
            flow_context(),
            type_string(),
        ));
    }

    #[DataProvider('string_data_provider')]
    public function test_as_string(mixed $input, ?string $default, ?string $expected): void
    {
        $parameter = new Parameter(lit($input));
        static::assertSame($expected, $parameter->asString(row(), flow_context(), $default));
    }

    public function test_constructor_with_mixed_value(): void
    {
        $parameter = new Parameter('direct_string');
        static::assertSame('direct_string', $parameter->eval(row(), flow_context()));

        $parameter = new Parameter(123);
        static::assertSame(123, $parameter->eval(row(), flow_context()));

        $parameter = new Parameter(true);
        static::assertTrue($parameter->eval(row(), flow_context()));
    }

    public function test_constructor_with_scalar_function(): void
    {
        $scalarFunction = lit('test');
        $parameter = new Parameter($scalarFunction);

        static::assertSame('test', $parameter->eval(row(), flow_context()));
    }

    public function test_eval_with_direct_value(): void
    {
        $parameter = new Parameter(lit('direct_value'));
        static::assertSame('direct_value', $parameter->eval(row(), flow_context()));

        $parameter = new Parameter(lit(42));
        static::assertSame(42, $parameter->eval(row(), flow_context()));
    }

    public function test_eval_with_reference(): void
    {
        $parameter = new Parameter(ref('column'));
        $result = $parameter->eval(row(str_entry('column', 'ref_value')), flow_context());

        static::assertSame('ref_value', $result);
    }
}
