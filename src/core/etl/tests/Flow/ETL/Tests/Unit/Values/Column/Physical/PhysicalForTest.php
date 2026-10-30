<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Physical;

use ArrayObject;
use BackedEnum;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Values\Column\Layout\BooleanLayout;
use Flow\ETL\Values\Column\Layout\FixedBinary16Layout;
use Flow\ETL\Values\Column\Layout\Float64Layout;
use Flow\ETL\Values\Column\Layout\Int32Layout;
use Flow\ETL\Values\Column\Layout\Int64Layout;
use Flow\ETL\Values\Column\Layout\Utf8Layout;
use Flow\ETL\Values\Column\Php\ListColumnBuilder;
use Flow\ETL\Values\Column\Php\MapColumnBuilder;
use Flow\ETL\Values\Column\Php\NullColumnBuilder;
use Flow\ETL\Values\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Values\Column\Php\StructColumnBuilder;
use Flow\ETL\Values\Column\Physical\DatePhysical;
use Flow\ETL\Values\Column\Physical\DateTimePhysical;
use Flow\ETL\Values\Column\Physical\EnumPhysical;
use Flow\ETL\Values\Column\Physical\HtmlDocumentPhysical;
use Flow\ETL\Values\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Values\Column\Physical\IdentityPhysical;
use Flow\ETL\Values\Column\Physical\JsonPhysical;
use Flow\ETL\Values\Column\Physical\ListPhysical;
use Flow\ETL\Values\Column\Physical\MapPhysical;
use Flow\ETL\Values\Column\Physical\NullPhysical;
use Flow\ETL\Values\Column\Physical\PhysicalFor;
use Flow\ETL\Values\Column\Physical\StructPhysical;
use Flow\ETL\Values\Column\Physical\TimePhysical;
use Flow\ETL\Values\Column\Physical\TimeZonePhysical;
use Flow\ETL\Values\Column\Physical\UuidPhysical;
use Flow\ETL\Values\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Values\Column\Physical\XmlElementPhysical;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use UnitEnum;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function in_array;
use function str_repeat;

final class PhysicalForTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, class-string}>
     */
    public static function types(): Generator
    {
        yield 'integer' => [type_integer(), IdentityPhysical::class];
        yield 'positive integer' => [type_positive_integer(), IdentityPhysical::class];
        yield 'float' => [type_float(), IdentityPhysical::class];
        yield 'boolean' => [type_boolean(), IdentityPhysical::class];
        yield 'string' => [type_string(), IdentityPhysical::class];
        yield 'non-empty string' => [type_non_empty_string(), IdentityPhysical::class];
        yield 'numeric string' => [type_numeric_string(), IdentityPhysical::class];
        yield 'class string' => [type_class_string(), IdentityPhysical::class];
        yield 'optional integer' => [type_optional(type_integer()), IdentityPhysical::class];
        yield 'datetime' => [type_datetime(), DateTimePhysical::class];
        yield 'date' => [type_date(), DatePhysical::class];
        yield 'time' => [type_time(), TimePhysical::class];
        yield 'uuid' => [type_uuid(), UuidPhysical::class];
        yield 'timezone' => [type_time_zone(), TimeZonePhysical::class];
        yield 'json' => [type_json(), JsonPhysical::class];
        yield 'enum' => [type_enum(BasicEnum::class), EnumPhysical::class];
        yield 'xml' => [type_xml(), XmlDocumentPhysical::class];
        yield 'xml element' => [type_xml_element(), XmlElementPhysical::class];
        yield 'html' => [type_html(), HtmlDocumentPhysical::class];
        yield 'html element' => [type_html_element(), HtmlElementPhysical::class];
        yield 'list of identities' => [type_list(type_optional(type_integer())), IdentityPhysical::class];
        yield 'list of lists of identities' => [type_list(type_list(type_string())), IdentityPhysical::class];
        yield 'list' => [type_list(type_date()), ListPhysical::class];
        yield 'map of identities' => [type_map(type_string(), type_integer()), IdentityPhysical::class];
        yield 'map' => [type_map(type_string(), type_uuid()), MapPhysical::class];
        yield 'structure of identities' => [
            type_structure(['a' => type_integer(), 'b' => type_list(type_string())]),
            IdentityPhysical::class,
        ];
        yield 'structure' => [type_structure(['a' => type_integer(), 'b' => type_date()]), StructPhysical::class];
        yield 'structure of a list' => [type_structure(['a' => type_list(type_date())]), StructPhysical::class];
        yield 'null' => [type_null(), NullPhysical::class];
    }

    /**
     * @return Generator<string, array{Type<mixed>, string}>
     */
    public static function refusals(): Generator
    {
        yield 'mixed' => [type_mixed(), 'a mixed element has no column kind, declare it or use json'];
        yield 'nested mixed' => [type_map(type_string(), type_mixed()), 'a mixed element has no column kind'];
        yield 'unit enum wildcard' => [type_enum(UnitEnum::class), 'declare the concrete enum class'];
        yield 'backed enum wildcard' => [type_enum(BackedEnum::class), 'declare the concrete enum class'];
        yield 'instance of' => [type_instance_of(stdClass::class), 'type object<stdClass> has no column kind'];
        yield 'structure element' => [type_structure(['a' => type_mixed()]), 'a mixed element has no column kind'];
    }

    /**
     * @return Generator<string, array{Type<mixed>, bool}>
     */
    public static function supported(): Generator
    {
        foreach (self::types() as $name => [$type]) {
            yield $name => [
                $type,
                !in_array($name, ['positive integer', 'non-empty string', 'class string'], true),
            ];
        }

        yield 'nullable union' => [type_union(type_string(), type_null()), true];
        yield 'array' => [type_array(), true];
        yield 'optional array' => [type_optional(type_array()), true];
        yield 'list of non-empty strings' => [type_list(type_non_empty_string()), true];
        yield 'list of optional integers' => [type_list(type_optional(type_integer())), true];
        yield 'structure of lists' => [
            type_structure(['a' => type_list(type_uuid()), 'b' => type_map(type_string(), type_date())]),
            true,
        ];
        yield 'mixed' => [type_mixed(), false];
        yield 'union' => [type_union(type_string(), type_integer()), false];
        yield 'object' => [type_object(), false];
        yield 'instance of' => [type_instance_of(ArrayObject::class), false];
        yield 'empty array' => [type_empty_array(), false];
        yield 'callable' => [type_callable(), false];
        yield 'scalar' => [type_scalar(), false];
        yield 'unit enum wildcard' => [type_enum(UnitEnum::class), false];
        yield 'backed enum wildcard' => [type_enum(BackedEnum::class), false];
        yield 'list of mixed' => [type_list(type_mixed()), false];
        yield 'list of arrays' => [type_list(type_array()), true];
        yield 'list of empty arrays' => [type_list(type_empty_array()), true];
        yield 'list of nullable unions' => [type_list(type_union(type_string(), type_null())), true];
        yield 'list of unions' => [type_list(type_union(type_string(), type_integer())), false];
        yield 'map of mixed' => [type_map(type_string(), type_mixed()), false];
        yield 'structure element' => [type_structure(['a' => type_integer(), 'b' => type_mixed()]), false];
        yield 'list of unit enums' => [type_list(type_enum(UnitEnum::class)), false];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('supported')]
    public function test_supports_agrees_with_definition_from_type_and_definition(Type $type, bool $supported): void
    {
        try {
            (new PhysicalFor())->definition(definition_from_type('value', $type));
            $defined = true;
        } catch (InvalidArgumentException|RuntimeException) {
            $defined = false;
        }

        static::assertSame($supported, (new PhysicalFor())->supports($type));
        static::assertSame($supported, $defined);
    }

    /**
     * @param Type<mixed> $type
     * @param class-string $class
     */
    #[DataProvider('types')]
    public function test_maps_the_type_to_its_physical(Type $type, string $class): void
    {
        static::assertInstanceOf($class, (new PhysicalFor())->physical($type));
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('refusals')]
    public function test_refuses(Type $type, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        (new PhysicalFor())->physical($type);
    }

    public function test_structure_elements_are_keyed_by_name(): void
    {
        static::assertSame(
            ['a', 'b'],
            array_keys((new PhysicalFor())->elements(type_structure(['a' => type_integer(), 'b' => type_string()]))),
        );
    }

    /**
     * @return Generator<string, array{Type<mixed>, class-string}>
     */
    public static function layouts(): Generator
    {
        yield 'integer' => [type_integer(), Int64Layout::class];
        yield 'positive integer' => [type_positive_integer(), Int64Layout::class];
        yield 'datetime' => [type_datetime(), Int64Layout::class];
        yield 'time' => [type_time(), Int64Layout::class];
        yield 'optional integer' => [type_optional(type_integer()), Int64Layout::class];
        yield 'float' => [type_float(), Float64Layout::class];
        yield 'date' => [type_date(), Int32Layout::class];
        yield 'boolean' => [type_boolean(), BooleanLayout::class];
        yield 'string' => [type_string(), Utf8Layout::class];
        yield 'non-empty string' => [type_non_empty_string(), Utf8Layout::class];
        yield 'numeric string' => [type_numeric_string(), Utf8Layout::class];
        yield 'class string' => [type_class_string(), Utf8Layout::class];
        yield 'timezone' => [type_time_zone(), Utf8Layout::class];
        yield 'json' => [type_json(), Utf8Layout::class];
        yield 'enum' => [type_enum(BasicEnum::class), Utf8Layout::class];
        yield 'xml' => [type_xml(), Utf8Layout::class];
        yield 'xml element' => [type_xml_element(), Utf8Layout::class];
        yield 'html' => [type_html(), Utf8Layout::class];
        yield 'html element' => [type_html_element(), Utf8Layout::class];
        yield 'uuid' => [type_uuid(), FixedBinary16Layout::class];
    }

    /**
     * @param Type<mixed> $type
     * @param class-string $class
     */
    #[DataProvider('layouts')]
    public function test_maps_the_type_to_its_layout(Type $type, string $class): void
    {
        static::assertInstanceOf($class, (new PhysicalFor())->layout($type));
    }

    public function test_a_container_has_no_scalar_layout(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('type list<integer> has no scalar layout');

        (new PhysicalFor())->layout(type_list(type_integer()));
    }

    public function test_picks_the_builder_of_the_column_kind(): void
    {
        $builders = new PhysicalFor();

        static::assertInstanceOf(ScalarColumnBuilder::class, $builders->builder(type_integer()));
        static::assertInstanceOf(
            ListColumnBuilder::class,
            $builders->builder(type_optional(type_list(type_integer()))),
        );
        static::assertInstanceOf(MapColumnBuilder::class, $builders->builder(type_map(type_string(), type_integer())));
        static::assertInstanceOf(
            StructColumnBuilder::class,
            $builders->builder(type_structure(['a' => type_integer()])),
        );
        static::assertInstanceOf(NullColumnBuilder::class, $builders->builder(type_null()));
    }

    public function test_an_optional_container_keeps_its_optional_type(): void
    {
        $builder = (new PhysicalFor())->builder(type_optional(type_list(type_integer())));
        $builder->appendPhysical(null);

        static::assertEquals(type_optional(type_list(type_integer())), $builder->finish()->type());
    }

    public function test_refuses_a_mixed_element(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('a mixed element has no column kind');

        (new PhysicalFor())->builder(type_list(type_mixed()));
    }

    /**
     * @return Generator<string, array{Type<mixed>, mixed, bool}>
     */
    public static function physicals(): Generator
    {
        yield 'null is any type\'s' => [type_integer(), null, true];
        yield 'int in an integer column' => [type_integer(), 1, true];
        yield 'string in an integer column' => [type_integer(), '1', false];
        yield 'int in a float column' => [type_float(), 1, false];
        yield 'float in a float column' => [type_float(), 1.5, true];
        yield 'bool' => [type_boolean(), true, true];
        yield 'datetime micros' => [type_datetime(), 1_700_000_000_000_000, true];
        yield 'datetime text' => [type_datetime(), '2026-01-01', false];
        yield 'uuid 16 bytes' => [type_uuid(), str_repeat("\0", 16), true];
        yield 'uuid text' => [type_uuid(), '00000000-0000-4000-8000-000000000000', false];
        yield 'json text' => [type_json(), '{"a":1}', true];
        yield 'optional unwrapped' => [type_optional(type_integer()), 1, true];
        yield 'anything in a null column' => [type_null(), 'x', true];
        yield 'list of ints' => [type_list(type_integer()), [1, null, 2], true];
        yield 'list with a wrong element' => [type_list(type_integer()), [1, '2'], false];
        yield 'list that is not an array' => [type_list(type_integer()), 'x', false];
        yield 'map values checked' => [type_map(type_string(), type_integer()), ['a' => 'b'], false];
        yield 'structure element checked' => [type_structure(['a' => type_integer()]), ['a' => 'x'], false];
        yield 'structure element absent' => [type_structure(['a' => type_integer()]), [], true];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('physicals')]
    public function test_accepts_the_kind_flow_php_stores(Type $type, mixed $physical, bool $accepts): void
    {
        static::assertSame($accepts, (new PhysicalFor())->accepts($type, $physical));
    }
}
