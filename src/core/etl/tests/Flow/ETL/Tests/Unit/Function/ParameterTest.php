<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Doctrine\Bulk\SQLParametersStyle;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use stdClass;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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
        static::assertSame([[]], (new Parameter(lit([])))->asArrays(RowsMother::sequentialIds(1), flow_context()));
    }

    public function test_as_array_with_valid_array(): void
    {
        static::assertSame(
            [['key' => 'value', 'number' => 42]],
            (new Parameter(lit(['key' => 'value', 'number' => 42])))->asArrays(
                RowsMother::sequentialIds(1),
                flow_context(),
            ),
        );
    }

    public function test_as_arrays_reads_a_json_column_as_arrays(): void
    {
        static::assertSame(
            [['a' => 1], null],
            (new Parameter((new ReferenceResolver())->resolve(
                ref('j'),
                array_to_rows([
                    ['j' => '{"a":1}'],
                    ['j' => null],
                ], schema(json_schema('j', nullable: true)))->schema(),
            )))->asArrays(array_to_rows([
                ['j' => '{"a":1}'],
                ['j' => null],
            ], schema(json_schema('j', nullable: true))), flow_context()),
        );
    }

    public function test_as_array_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an array')))->asArrays(RowsMother::sequentialIds(1), flow_context());
    }

    public function test_as_array_propagates_null(): void
    {
        static::assertSame([null], (new Parameter(lit(null)))->asArrays(RowsMother::sequentialIds(1), flow_context()));
    }

    /**
     * @param null|bool $expected
     */
    #[DataProvider('boolean_data_provider')]
    public function test_as_boolean(mixed $input, ?bool $expected): void
    {
        static::assertSame(
            [$expected],
            (new Parameter(lit($input)))->asBooleans(RowsMother::sequentialIds(1), flow_context()),
        );
    }

    public function test_as_booleans_reads_a_boolean_column_as_is(): void
    {
        static::assertSame(
            [true, null, false],
            (new Parameter((new ReferenceResolver())->resolve(
                ref('b'),
                array_to_rows([
                    ['b' => true],
                    ['b' => null],
                    ['b' => false],
                ], schema(bool_schema('b', nullable: true)))->schema(),
            )))->asBooleans(array_to_rows([
                ['b' => true],
                ['b' => null],
                ['b' => false],
            ], schema(bool_schema('b', nullable: true))), flow_context()),
        );
    }

    public function test_as_boolean_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(['value'])))->asBooleans(RowsMother::sequentialIds(1), flow_context());
    }

    public function test_as_enum_propagates_null(): void
    {
        static::assertSame(
            [null],
            (new Parameter(lit(null)))->asEnums(
                RowsMother::sequentialIds(1),
                flow_context(),
                SQLParametersStyle::class,
            ),
        );
    }

    public function test_as_enum_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an enum')))->asEnums(
            RowsMother::sequentialIds(1),
            flow_context(),
            SQLParametersStyle::class,
        );
    }

    public function test_as_enum_with_valid_enum(): void
    {
        static::assertSame(
            [SQLParametersStyle::NAMED],
            (new Parameter(lit(SQLParametersStyle::NAMED)))->asEnums(
                RowsMother::sequentialIds(1),
                flow_context(),
                SQLParametersStyle::class,
            ),
        );
    }

    public function test_as_enum_with_wrong_enum_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(SQLParametersStyle::NAMED)))->asEnums(
            RowsMother::sequentialIds(1),
            flow_context(),
            StringStyles::class,
        );
    }

    public function test_as_instance_of_propagates_null(): void
    {
        static::assertSame(
            [null],
            (new Parameter(lit(null)))->asInstancesOf(
                RowsMother::sequentialIds(1),
                flow_context(),
                DateTimeImmutable::class,
            ),
        );
    }

    public function test_as_instance_of_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an object')))->asInstancesOf(
            RowsMother::sequentialIds(1),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    public function test_as_instance_of_with_valid_object(): void
    {
        $dateTime = new DateTimeImmutable('2023-01-01');
        $rows = array_to_rows([['at' => $dateTime]], schema(datetime_schema('at')));

        static::assertEquals(
            [$dateTime],
            (new Parameter((new ReferenceResolver())->resolve(ref('at'), $rows->schema())))->asInstancesOf(
                $rows,
                flow_context(),
                DateTimeImmutable::class,
            ),
        );
        static::assertEquals(
            [$dateTime],
            (new Parameter((new ReferenceResolver())->resolve(ref('at'), $rows->schema())))->asInstancesOf(
                $rows,
                flow_context(),
                DateTimeInterface::class,
            ),
        );
    }

    #[DataProvider('int_data_provider')]
    public function test_as_int(mixed $input, ?int $default, ?int $expected): void
    {
        static::assertSame(
            [$expected],
            (new Parameter(lit($input)))->asInts(RowsMother::sequentialIds(1), flow_context(), $default),
        );
    }

    public function test_as_ints_reads_an_integer_column_as_is_and_defaults_its_nulls(): void
    {
        $rows = array_to_rows([['n' => 1], ['n' => null]], schema(int_schema('n', nullable: true)));

        static::assertSame(
            [1, null],
            (new Parameter((new ReferenceResolver())->resolve(ref('n'), $rows->schema())))->asInts(
                $rows,
                flow_context(),
            ),
        );
        static::assertSame(
            [1, 7],
            (new Parameter((new ReferenceResolver())->resolve(ref('n'), $rows->schema())))->asInts(
                $rows,
                flow_context(),
                7,
            ),
        );
    }

    public function test_as_int_throws_on_a_malformed_value(): void
    {
        // A default never applies to a malformed value - only to a NULL input.
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(3.14)))->asInts(RowsMother::sequentialIds(1), flow_context(), 99);
    }

    public function test_as_list_of_objects_propagates_null(): void
    {
        static::assertSame(
            [null],
            (new Parameter(lit(null)))->asListsOfObjects(
                RowsMother::sequentialIds(1),
                flow_context(),
                DateTimeImmutable::class,
            ),
        );
    }

    public function test_as_list_of_objects_throws_on_a_malformed_element(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit([new DateTimeImmutable('2023-01-01'), 'not an object'])))->asListsOfObjects(
            RowsMother::sequentialIds(1),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    public function test_as_list_of_objects_throws_on_a_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not an array')))->asListsOfObjects(
            RowsMother::sequentialIds(1),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    public function test_as_list_of_objects_with_empty_array(): void
    {
        static::assertSame(
            [[]],
            (new Parameter(lit([])))->asListsOfObjects(
                RowsMother::sequentialIds(1),
                flow_context(),
                DateTimeImmutable::class,
            ),
        );
    }

    public function test_as_list_of_objects_with_valid_array(): void
    {
        $date1 = new DateTimeImmutable('2023-01-01');
        $date2 = new DateTimeImmutable('2023-01-02');

        static::assertEquals(
            [[$date1, $date2]],
            (new Parameter(lit([$date1, $date2])))->asListsOfObjects(
                RowsMother::sequentialIds(1),
                flow_context(),
                DateTimeImmutable::class,
            ),
        );
    }

    public function test_as_list_of_objects_with_wrong_object_type(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit([new DateTimeImmutable('2023-01-01'), new stdClass()])))->asListsOfObjects(
            RowsMother::sequentialIds(1),
            flow_context(),
            DateTimeImmutable::class,
        );
    }

    #[DataProvider('number_data_provider')]
    public function test_as_number(mixed $input, int|float|null $default, int|float|null $expected): void
    {
        static::assertSame(
            [$expected],
            (new Parameter(lit($input)))->asNumbers(RowsMother::sequentialIds(1), flow_context(), $default),
        );
    }

    public function test_as_numbers_reads_numeric_columns_as_is_and_defaults_their_nulls(): void
    {
        $rows = array_to_rows([['f' => 1.5], ['f' => null]], schema(float_schema('f', nullable: true)));

        static::assertSame(
            [1.5, null],
            (new Parameter((new ReferenceResolver())->resolve(ref('f'), $rows->schema())))->asNumbers(
                $rows,
                flow_context(),
            ),
        );
        static::assertSame(
            [1.5, 0],
            (new Parameter((new ReferenceResolver())->resolve(ref('f'), $rows->schema())))->asNumbers(
                $rows,
                flow_context(),
                0,
            ),
        );
    }

    public function test_as_number_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit('not numeric')))->asNumbers(RowsMother::sequentialIds(1), flow_context(), 99);
    }

    #[DataProvider('string_data_provider')]
    public function test_as_string(mixed $input, ?string $default, ?string $expected): void
    {
        static::assertSame(
            [$expected],
            (new Parameter(lit($input)))->asStrings(RowsMother::sequentialIds(1), flow_context(), $default),
        );
    }

    public function test_as_strings_reads_a_string_column_as_is_and_defaults_its_nulls(): void
    {
        $rows = array_to_rows([['s' => 'a'], ['s' => null]], schema(str_schema('s', nullable: true)));

        static::assertSame(
            ['a', null],
            (new Parameter((new ReferenceResolver())->resolve(ref('s'), $rows->schema())))->asStrings(
                $rows,
                flow_context(),
            ),
        );
        static::assertSame(
            ['a', 'x'],
            (new Parameter((new ReferenceResolver())->resolve(ref('s'), $rows->schema())))->asStrings(
                $rows,
                flow_context(),
                'x',
            ),
        );
    }

    public function test_as_string_throws_on_a_malformed_value(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Parameter(lit(42)))->asStrings(RowsMother::sequentialIds(1), flow_context(), 'default');
    }

    public function test_as_types_accepts_the_first_type_that_matches(): void
    {
        static::assertSame(
            ['42'],
            (new Parameter((new ReferenceResolver())->resolve(
                ref('value'),
                array_to_rows([['value' => '42']], schema(str_schema('value')))->schema(),
            )))->asTypes(
                array_to_rows([['value' => '42']], schema(str_schema('value'))),
                flow_context(),
                type_string(),
                type_integer(),
            ),
        );
    }

    public function test_as_types_propagates_null(): void
    {
        static::assertSame(
            [null],
            (new Parameter(lit(null)))->asTypes(RowsMother::sequentialIds(1), flow_context(), type_integer()),
        );
    }

    public function test_as_types_throws_when_no_type_matches(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected one of "integer", "boolean", got "string". (row 0)');

        (new Parameter((new ReferenceResolver())->resolve(
            ref('value'),
            array_to_rows([['value' => '42']], schema(str_schema('value')))->schema(),
        )))->asTypes(
            array_to_rows([['value' => '42']], schema(str_schema('value'))),
            flow_context(),
            type_integer(),
            type_boolean(),
        );
    }

    public function test_a_refused_value_names_its_row(): void
    {
        try {
            (new Parameter((new ReferenceResolver())->resolve(
                ref('value'),
                array_to_rows([
                    ['value' => '1'],
                    ['value' => '2'],
                    ['value' => 'x'],
                ], schema(str_schema('value')))->schema(),
            )))->asInts(array_to_rows([
                ['value' => '1'],
                ['value' => '2'],
                ['value' => 'x'],
            ], schema(str_schema('value'))), flow_context());
            static::fail('expected an EvaluationException');
        } catch (EvaluationException $e) {
            static::assertSame(0, $e->rowIndex);
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
        }
    }

    public function test_column_evaluates_the_function(): void
    {
        $rows = array_to_rows([['column' => 'ref_value']], schema(str_schema('column')));

        static::assertSame(
            ['ref_value'],
            (new Parameter((new ReferenceResolver())->resolve(ref('column'), $rows->schema())))
                ->column($rows, flow_context())
                ->values(),
        );
    }

    public function test_constructor_with_mixed_value(): void
    {
        static::assertSame(
            ['direct_string'],
            (new Parameter('direct_string'))->values(RowsMother::sequentialIds(1), flow_context()),
        );
        static::assertSame([123], (new Parameter(123))->values(RowsMother::sequentialIds(1), flow_context()));
        static::assertSame([true], (new Parameter(true))->values(RowsMother::sequentialIds(1), flow_context()));
    }

    public function test_values_read_an_array_result_back_as_arrays(): void
    {
        static::assertSame(
            [['name' => 'a']],
            (new Parameter((new ReferenceResolver())->resolve(
                ref('j')->jsonDecode(),
                array_to_rows([[
                    'j' => '{"name":"a"}',
                ]], schema(json_schema('j')))->schema(),
            )))->values(array_to_rows([[
                'j' => '{"name":"a"}',
            ]], schema(json_schema('j'))), flow_context()),
        );
    }
}
