<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use DateTimeImmutable;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Mother\ColumnMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

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
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_xml_element;

final class ComparableValuesTest extends TestCase
{
    /**
     * @return Generator<string, array{Definition<mixed>, list<mixed>}>
     */
    public static function ordered(): Generator
    {
        yield 'integer' => [int_schema('a', nullable: true), [2, null, 1]];
        yield 'float' => [float_schema('a'), [1.5, 0.5]];
        yield 'boolean' => [bool_schema('a'), [true, false]];
        yield 'string' => [str_schema('a'), ['b', 'a']];
        yield 'datetime' => [
            datetime_schema('a'),
            [new DateTimeImmutable('2024-01-02'), new DateTimeImmutable('2024-01-01')],
        ];
        yield 'time' => [time_schema('a'), ['PT2H', 'PT1H']];
    }

    /**
     * @return Generator<string, array{Definition<mixed>, list<mixed>}>
     */
    public static function equal_by_physical(): Generator
    {
        yield 'uuid' => [uuid_schema('a'), ['00000000-0000-4000-8000-000000000001']];
        yield 'json' => [json_schema('a'), ['{"a":1}']];
        yield 'enum' => [enum_schema('a', BackedStringEnum::class), [BackedStringEnum::one]];
        yield 'xml' => [xml_schema('a'), ['<a>1</a>']];
    }

    /**
     * @return Generator<string, array{Definition<mixed>, list<mixed>}>
     */
    public static function by_value(): Generator
    {
        yield 'timezone' => [time_zone_schema('a'), ['UTC']];
        yield 'list' => [list_schema('a', type_list(type_integer())), [[1, 2]]];
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_compares_by_physical(): void
    {
        $document = ColumnMother::of(
            html_schema('a'),
            ['<!DOCTYPE html><html><head></head><body><p>a</p></body></html>'],
        );
        $element = ColumnMother::of(html_element_schema('a'), [
            type_html_element()->cast('<div><p>a</p></div>')->firstElementChild,
            type_html_element()->cast('<section><p>a</p></section>')->firstElementChild,
        ]);

        static::assertSame($document->physicals(), (new ComparableValues())->equality($document));
        static::assertSame(['<p>a</p>', '<p>a</p>'], (new ComparableValues())->equality($element));
    }

    public function test_xml_elements_of_different_documents_compare_by_their_markup(): void
    {
        $first = type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild;
        $second = type_xml_element()->cast('<section><p>a</p></section>')->firstElementChild;
        $elements = ColumnMother::of(xml_element_schema('a', nullable: true), [$first, $second, null]);
        $lists = ColumnMother::of(list_schema('a', type_list(type_xml_element())), [[$first], [$second]]);

        static::assertTrue((new ComparableValues())->equalByPhysical($elements->type()));
        static::assertEquals($elements->values(), (new ComparableValues())->ordering($elements));
        static::assertSame(['<p>a</p>', '<p>a</p>', null], (new ComparableValues())->equality($elements));
        static::assertSame(
            [['<p>a</p>'], ['<p>a</p>']],
            (new ComparableValues())->equalities($lists->type(), $lists->physicals()),
        );
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    #[DataProvider('by_value')]
    public function test_other_types_compare_their_values(Definition $definition, array $values): void
    {
        $column = ColumnMother::of($definition, $values);

        static::assertFalse((new ComparableValues())->orderedByPhysical($column->type()));
        static::assertEquals($column->values(), (new ComparableValues())->ordering($column));
        static::assertEquals($column->values(), (new ComparableValues())->equality($column));
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    #[DataProvider('equal_by_physical')]
    public function test_equality_only_types_order_by_value(Definition $definition, array $values): void
    {
        $column = ColumnMother::of($definition, $values);

        static::assertFalse((new ComparableValues())->orderedByPhysical($column->type()));
        static::assertEquals($column->values(), (new ComparableValues())->ordering($column));
        static::assertSame($column->physicals(), (new ComparableValues())->equality($column));
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    #[DataProvider('ordered')]
    public function test_ordered_types_read_physicals(Definition $definition, array $values): void
    {
        $column = ColumnMother::of($definition, $values);

        static::assertTrue((new ComparableValues())->orderedByPhysical($column->type()));
        static::assertTrue((new ComparableValues())->equalByPhysical($column->type()));
        static::assertSame($column->physicals(), (new ComparableValues())->ordering($column));
        static::assertSame($column->physicals(), (new ComparableValues())->equality($column));
    }

    public function test_a_date_compares_on_the_datetime_microsecond_scale(): void
    {
        $column = ColumnMother::of(date_schema('a', nullable: true), [new DateTimeImmutable('2024-01-02'), null]);

        static::assertSame([1_704_153_600_000_000, null], (new ComparableValues())->ordering($column));
        static::assertSame([1_704_153_600_000_000, null], (new ComparableValues())->equality($column));
        static::assertSame(
            (new ComparableValues())->equality(ColumnMother::of(datetime_schema('a'), [new DateTimeImmutable(
                '2024-01-02 00:00:00 UTC',
            )])),
            [(new ComparableValues())->equality($column)[0]],
        );
    }
}
