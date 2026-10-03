<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\Physical\HtmlDocumentPhysical;
use Flow\ETL\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Double\ForeignColumnStub;
use Flow\ETL\Tests\Double\ForeignTypeDefinition;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Tests\Mother\DateIntervalMother;
use Flow\ETL\Tests\Mother\XmlMother;
use Flow\Types\Exception\CastingException;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;
use stdClass;
use UnitEnum;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class PhpBackendTest extends TestCase
{
    /**
     * @return Generator<string, array{Definition<mixed>, mixed, mixed}>
     */
    public static function definitions(): Generator
    {
        $structure = type_structure([
            'id' => type_integer(),
            'note' => structure_element('note', type_optional(type_string()), optional: true),
            'tag' => type_optional(type_string()),
        ]);

        yield 'integer' => [int_schema('a'), '5', 5];
        yield 'float' => [float_schema('a'), 1.5, 1.5];
        yield 'boolean' => [bool_schema('a'), true, true];
        yield 'string' => [str_schema('a'), 'text', 'text'];
        yield 'datetime' => [
            datetime_schema('a'),
            '2024-01-02 03:04:05.123456',
            new DateTimeImmutable('2024-01-02 03:04:05.123456 UTC'),
        ];
        yield 'datetime in the column zone' => [
            datetime_schema('a', zone: 'Europe/Warsaw'),
            new DateTimeImmutable('2024-01-02 03:04:05.5 UTC'),
            new DateTimeImmutable('2024-01-02 04:04:05.5', new DateTimeZone('Europe/Warsaw')),
        ];
        yield 'datetime one microsecond before the epoch' => [
            datetime_schema('a'),
            new DateTimeImmutable('@-0.000001'),
            new DateTimeImmutable('1969-12-31 23:59:59.999999 UTC'),
        ];
        yield 'datetime one and a half seconds before the epoch' => [
            datetime_schema('a'),
            new DateTimeImmutable('@-1.500000'),
            new DateTimeImmutable('1969-12-31 23:59:58.5 UTC'),
        ];
        yield 'date east of UTC keeps its calendar day' => [
            date_schema('a'),
            new DateTimeImmutable('2024-03-05 00:30:00', new DateTimeZone('Asia/Tokyo')),
            new DateTimeImmutable('2024-03-05 00:00:00 UTC'),
        ];
        yield 'date before the epoch' => [
            date_schema('a'),
            new DateTimeImmutable('1969-12-29 00:00:00 UTC'),
            new DateTimeImmutable('1969-12-29 00:00:00 UTC'),
        ];
        yield 'time folds days into hours' => [
            time_schema('a'),
            DateIntervalMother::of('P1DT2H3M4S', 0.25, 1),
            DateIntervalMother::of('PT26H3M4S', 0.25, 1),
        ];
        yield 'uuid' => [
            uuid_schema('a'),
            '6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f',
            new Uuid('6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f'),
        ];
        yield 'timezone' => [time_zone_schema('a'), 'Europe/Warsaw', new DateTimeZone('Europe/Warsaw')];
        yield 'json object' => [json_schema('a'), '{"a":1}', Json::fromString('{"a":1}')];
        yield 'json list' => [json_schema('a'), [1, 2], Json::fromString('[1,2]')];
        yield 'enum' => [enum_schema('a', BackedStringEnum::class), 'one', BackedStringEnum::one];
        yield 'xml' => [xml_schema('a'), '<root><a>1</a></root>', XmlMother::document('<root><a>1</a></root>')];
        yield 'xml element' => [
            xml_element_schema('a'),
            XmlMother::document('<root><a>1</a></root>')->documentElement,
            XmlMother::document('<root><a>1</a></root>')->documentElement,
        ];
        yield 'list with a null element' => [
            list_schema('a', type_list(type_optional(type_integer()))),
            [1, null, 3],
            [1, null, 3],
        ];
        yield 'list of datetimes in the column zone' => [
            list_schema('a', type_list(type_datetime('Europe/Warsaw'))),
            [new DateTimeImmutable('2024-01-02 03:04:05 UTC')],
            [new DateTimeImmutable('2024-01-02 04:04:05', new DateTimeZone('Europe/Warsaw'))],
        ];
        yield 'map with a null value' => [
            map_schema('a', type_map(type_string(), type_optional(type_integer()))),
            ['x' => 1, 'y' => null],
            ['x' => 1, 'y' => null],
        ];
        yield 'structure with every element' => [
            structure_schema('a', $structure),
            ['id' => 1, 'note' => 'x', 'tag' => 'y'],
            ['id' => 1, 'note' => 'x', 'tag' => 'y'],
        ];
        yield 'structure: an optional nullable element given null reads back absent' => [
            structure_schema('a', $structure),
            ['id' => 1, 'note' => null, 'tag' => null],
            ['id' => 1, 'tag' => null],
        ];
        yield 'structure: an absent optional element stays absent' => [
            structure_schema('a', $structure),
            ['id' => 1, 'tag' => 'y'],
            ['id' => 1, 'tag' => 'y'],
        ];
    }

    /**
     * @return Generator<string, array{Definition<mixed>}>
     */
    public static function not_null_definitions(): Generator
    {
        foreach (self::definitions() as $name => [$definition]) {
            yield $name => [$definition];
        }
    }

    /**
     * @return Generator<string, array{Definition<mixed>, string}>
     */
    public static function refusals(): Generator
    {
        yield 'a definition outside the nineteen' => [
            new ForeignTypeDefinition('a', type_integer()),
            'Row does not match its schema: column "a": integer cannot be a batch column, only the 19 Flow definitions have a column kind',
        ];
        yield 'a mixed element' => [
            list_schema('a', type_list(type_mixed())),
            'Row does not match its schema: column "a": list<mixed> cannot be a batch column, a mixed element has no column kind, declare it or use json',
        ];
        yield 'a wildcard enum' => [
            enum_schema('a', UnitEnum::class),
            'Row does not match its schema: column "a": enum<UnitEnum> cannot be a batch column, declare the concrete enum class',
        ];
        yield 'a type without a column kind' => [
            list_schema('a', type_list(type_instance_of(stdClass::class))),
            'Row does not match its schema: column "a": list<object<stdClass>> cannot be a batch column, type object<stdClass> has no column kind',
        ];
    }

    /**
     * @return Generator<string, array{Definition<mixed>, mixed, bool}>
     */
    public static function temporal_range(): Generator
    {
        yield 'datetime at the upper bound' => [
            datetime_schema('a'),
            new DateTimeImmutable('@9223372036854.775807'),
            true,
        ];
        yield 'datetime past the upper bound' => [
            datetime_schema('a'),
            new DateTimeImmutable('@9223372036854.775808'),
            false,
        ];
        yield 'datetime at the lower bound' => [datetime_schema('a'), new DateTimeImmutable('@-9223372036854'), true];
        yield 'datetime past the lower bound' => [
            datetime_schema('a'),
            new DateTimeImmutable('@-9223372036855'),
            false,
        ];
        yield 'date at the upper bound' => [
            date_schema('a'),
            new DateTimeImmutable('@' . ((2 ** 31 - 1) * 86_400)),
            true,
        ];
        yield 'date past the upper bound' => [
            date_schema('a'),
            new DateTimeImmutable('@' . ((2 ** 31) * 86_400)),
            false,
        ];
        yield 'date at the lower bound' => [date_schema('a'), new DateTimeImmutable('@' . (-2 ** 31 * 86_400)), true];
        yield 'date past the lower bound' => [
            date_schema('a'),
            new DateTimeImmutable('@' . ((-2 ** 31 - 1) * 86_400)),
            false,
        ];
        yield 'time at the upper bound' => [
            time_schema('a'),
            DateIntervalMother::seconds(9_223_372_036_854, 0.775807, 0),
            true,
        ];
        yield 'time past the upper bound' => [
            time_schema('a'),
            DateIntervalMother::seconds(9_223_372_036_854, 0.775808, 0),
            false,
        ];
        yield 'negative time at the bound' => [
            time_schema('a'),
            DateIntervalMother::seconds(9_223_372_036_854, 0.775807, 1),
            true,
        ];
        yield 'negative time past the bound' => [
            time_schema('a'),
            DateIntervalMother::seconds(9_223_372_036_854, 0.775808, 1),
            false,
        ];
        yield 'time with a negative component past the bound' => [
            time_schema('a'),
            DateInterval::createFromDateString('-9223372036860 seconds'),
            false,
        ];
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('definitions')]
    public function test_round_trips(Definition $definition, mixed $value, mixed $expected): void
    {
        static::assertEquals($expected, ColumnMother::of($definition, [$value])->value(0));
        static::assertEquals([$expected], ColumnMother::of($definition, [$value])->values());
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('definitions')]
    public function test_round_trips_null_under_a_nullable_definition(
        Definition $definition,
        mixed $value,
        mixed $expected,
    ): void {
        $column = ColumnMother::of($definition->makeNullable(), [$value, null]);

        static::assertEquals([$expected, null], $column->values());
        static::assertSame(1, $column->nullCount());
        static::assertTrue($column->isNull(1));
        static::assertFalse($column->isNull(0));
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('not_null_definitions')]
    public function test_refuses_null_under_a_not_null_definition(Definition $definition): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('could not convert null to');

        (new PhpBackend())
            ->builder($definition)
            ->append(null);
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_round_trips(): void
    {
        $document = type_instance_of(HTMLDocument::class)->assert(ColumnMother::of(
            html_schema('a'),
            ['<!DOCTYPE html><html><head></head><body><p>a</p></body></html>'],
        )->value(0));
        $element = type_instance_of(HTMLElement::class)->assert(ColumnMother::of(html_element_schema('a'), [
            type_html_element()->cast('<p>a</p>'),
        ])->value(0));

        static::assertStringContainsString(
            '<p>a</p>',
            type_string()->assert((new HtmlDocumentPhysical())->toPhysical($document)),
        );
        static::assertSame(
            '<p>a</p>',
            (new HtmlElementPhysical())->markup(type_string()->assert((new HtmlElementPhysical())->toPhysical(
                $element,
            ))),
        );
    }

    public function test_the_null_definition_holds_nulls_only(): void
    {
        $column = ColumnMother::of(null_schema('a'), [null, null, null]);

        static::assertSame(3, $column->count());
        static::assertSame(3, $column->nullCount());
        static::assertSame([null, null, null], $column->values());
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('refusals')]
    public function test_refuses(Definition $definition, string $message): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage($message);

        (new PhpBackend())->builder($definition);
    }

    public function test_time_refuses_months(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Relative DateInterval (with months/years) can't be stored in a time column");

        (new PhpBackend())
            ->builder(time_schema('a'))
            ->append('P1M');
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('temporal_range')]
    public function test_temporal_range(Definition $definition, mixed $value, bool $accepted): void
    {
        if (!$accepted) {
            $this->expectException(SchemaMismatchException::class);
            $this->expectExceptionMessage('outside the');
        }

        static::assertIsInt(ColumnMother::of($definition, [$value])->at(0));
    }

    public function test_date_reads_back_in_utc(): void
    {
        $date = type_instance_of(DateTimeImmutable::class)->assert(ColumnMother::of(date_schema('a'), [
            new DateTimeImmutable('2024-03-05 00:30:00', new DateTimeZone('Asia/Tokyo')),
        ])->value(0));

        static::assertInstanceOf(DateTimeImmutable::class, $date);
        static::assertSame('UTC', $date->getTimezone()->getName());
        static::assertSame('2024-03-05 00:00:00', $date->format('Y-m-d H:i:s'));
    }

    public function test_datetime_reads_back_in_the_column_zone(): void
    {
        $datetime = type_instance_of(DateTimeImmutable::class)->assert(ColumnMother::of(
            datetime_schema('a', zone: 'Europe/Warsaw'),
            [
                new DateTimeImmutable('2024-01-02 03:04:05 UTC'),
            ],
        )->value(0));

        static::assertInstanceOf(DateTimeImmutable::class, $datetime);
        static::assertSame('Europe/Warsaw', $datetime->getTimezone()->getName());
    }

    public function test_time_reads_back_with_days_folded_and_no_days_count(): void
    {
        $time = type_instance_of(DateInterval::class)->assert(ColumnMother::of(time_schema('a'), [new DateInterval(
            'P2DT1H',
        )])->value(0));

        static::assertInstanceOf(DateInterval::class, $time);
        static::assertSame(0, $time->d);
        static::assertSame(49, $time->h);
        static::assertFalse($time->days);
    }

    public function test_constant_holds_one_value(): void
    {
        $column = (new PhpBackend())->constant(int_schema('a'), '7', 3);

        static::assertSame(3, $column->count());
        static::assertSame(0, $column->nullCount());
        static::assertSame([7, 7, 7], $column->values());
        static::assertSame(7, $column->value(2));
        static::assertSame(7, $column->at(1));
    }

    public function test_constant_null(): void
    {
        $column = (new PhpBackend())->constant(int_schema('a', nullable: true), null, 2);

        static::assertSame(2, $column->nullCount());
        static::assertSame([null, null], $column->values());
        static::assertTrue($column->isNull(0));
    }

    public function test_constant_refuses_null_under_a_not_null_definition(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a": could not convert null to integer, column is not nullable');

        (new PhpBackend())->constant(int_schema('a'), null, 2);
    }

    public function test_constant_null_under_the_null_definition(): void
    {
        static::assertSame(
            2,
            (new PhpBackend())
                ->constant(null_schema('a'), null, 2)
                ->nullCount(),
        );
    }

    public function test_constant_slice_take_and_concat(): void
    {
        $backend = new PhpBackend();
        $column = $backend->constant(int_schema('a'), 7, 5);

        static::assertSame([7, 7], $column->slice(1, 2)->values());
        static::assertSame([7, 7, 7], $column->take([0, 4, 2])->values());
        static::assertSame([7, 7, 7, 7, 7, 7], $column->concat($backend->constant(int_schema('a'), 7, 1))->values());
        static::assertSame([7, 7, 7, 7, 7, 8], $column->concat($backend->constant(int_schema('a'), 8, 1))->values());
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('definitions')]
    public function test_decode_round_trips(Definition $definition, mixed $value, mixed $expected): void
    {
        $nullable = $definition->makeNullable();
        $column = ColumnMother::of($nullable, [$value, null, $value]);
        $backend = new PhpBackend();

        foreach ([$column, $column->slice(1, 2), $column->take([2, 1])] as $variant) {
            static::assertEquals(
                $variant->values(),
                $backend->decode($nullable, $variant->encode(), $variant->count(), $variant->nullCount())->values(),
            );
        }
    }

    public function test_decode_round_trips_a_null_column(): void
    {
        $column = ColumnMother::of(null_schema('a'), [null, null]);

        static::assertSame(
            [null, null],
            (new PhpBackend())
                ->decode(null_schema('a'), $column->encode(), 2, 2)
                ->values(),
        );
    }

    /**
     * @return Generator<string, array{Definition<mixed>, list<string>, int, int, string}>
     */
    public static function corrupt_buffers(): Generator
    {
        yield 'too few buffers' => [int_schema('a'), [''], 1, 0, 'Column buffers exhausted'];
        yield 'short fixed buffer' => [
            int_schema('a'),
            ['', "\x01\x00"],
            1,
            0,
            'Int64 values buffer of 2 bytes, expected 8 for 1 rows',
        ];
        yield 'offsets not from 0' => [
            str_schema('a'),
            ['', "\x01\x00\x00\x00\x02\x00\x00\x00", 'ab'],
            1,
            0,
            'Utf8 offsets start at 1, not 0',
        ];
        yield 'offsets not monotonic' => [
            str_schema('a'),
            ['', "\x00\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00", 'ab'],
            2,
            0,
            'Utf8 offsets are not monotonic',
        ];
        yield 'offsets past the data' => [
            str_schema('a'),
            ['', "\x00\x00\x00\x00\x03\x00\x00\x00", 'ab'],
            1,
            0,
            'Utf8 data buffer of 2 bytes, the last offset is 3',
        ];
        yield 'map entries validity present' => [
            map_schema('a', type_map(type_string(), type_integer())),
            ['', "\x00\x00\x00\x00\x00\x00\x00\x00", "\x01", '', "\x00\x00\x00\x00", '', '', ''],
            1,
            0,
            'Map entries validity must be omitted, got 1 bytes',
        ];
        yield 'null count disagreeing with the bitmap' => [
            int_schema('a', nullable: true),
            ["\x01", "\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"],
            2,
            0,
            'Column null count 0 disagrees with its validity bitmap (1 nulls in 2 rows)',
        ];
        yield 'short validity bitmap' => [
            int_schema('a', nullable: true),
            ['', ''],
            9,
            0,
            'Int64 values buffer of 0 bytes, expected 72 for 9 rows',
        ];
        yield 'null column with a wrong null count' => [
            null_schema('a'),
            [],
            2,
            1,
            'Null column of 2 rows carries a null count of 1',
        ];
        yield 'leftover buffers' => [
            int_schema('a'),
            ['', "\x01\x00\x00\x00\x00\x00\x00\x00", ''],
            1,
            0,
            'Column "a": 1 buffers left after decoding',
        ];
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<string> $buffers
     */
    #[DataProvider('corrupt_buffers')]
    public function test_decode_refuses(
        Definition $definition,
        array $buffers,
        int $count,
        int $nullCount,
        string $message,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        (new PhpBackend())->decode($definition, $buffers, $count, $nullCount);
    }

    public function test_adopt_returns_its_own_column(): void
    {
        $column = ColumnMother::of(int_schema('a'), [1, 2]);

        static::assertSame($column, (new PhpBackend())->adopt(int_schema('a'), $column));
    }

    public function test_adopt_copies_a_foreign_column(): void
    {
        $foreign = new ForeignColumnStub(ColumnMother::of(str_schema('a', nullable: true), ['x', null, 'z']));

        $adopted = (new PhpBackend())->adopt(str_schema('a', nullable: true), $foreign);

        static::assertNotSame($foreign, $adopted);
        static::assertSame(['x', null, 'z'], $adopted->values());
        static::assertSame(1, $adopted->nullCount());
    }

    public function test_adopt_copies_an_empty_foreign_column(): void
    {
        $foreign = new ForeignColumnStub(ColumnMother::of(int_schema('a'), []));

        $adopted = (new PhpBackend())->adopt(int_schema('a'), $foreign);

        static::assertNotSame($foreign, $adopted);
        static::assertSame(0, $adopted->count());
    }

    public function test_adopt_refuses_a_null_under_not_null(): void
    {
        $foreign = new ForeignColumnStub(ColumnMother::of(int_schema('a', nullable: true), [1, null]));

        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(ColumnMismatchException::valueDoesNotMatch(int_schema('a'), null)->getMessage());

        (new PhpBackend())->adopt(int_schema('a'), $foreign);
    }

    public function test_adopt_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'column "a": mixed cannot be a batch column, an untyped function result exists only inside function evaluation',
        );

        (new PhpBackend())->adopt(int_schema('a'), new ValueColumn([1, 2]));
    }

    public function test_allocated_bytes_is_zero(): void
    {
        static::assertSame(0, (new PhpBackend())->allocatedBytes());
    }
}
