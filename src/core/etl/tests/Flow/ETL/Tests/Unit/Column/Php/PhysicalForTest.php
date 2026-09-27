<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use BackedEnum;
use Flow\ETL\Column\Php\DatePhysical;
use Flow\ETL\Column\Php\DateTimePhysical;
use Flow\ETL\Column\Php\EnumPhysical;
use Flow\ETL\Column\Php\HtmlDocumentPhysical;
use Flow\ETL\Column\Php\HtmlElementPhysical;
use Flow\ETL\Column\Php\IdentityPhysical;
use Flow\ETL\Column\Php\JsonPhysical;
use Flow\ETL\Column\Php\ListPhysical;
use Flow\ETL\Column\Php\MapPhysical;
use Flow\ETL\Column\Php\NullPhysical;
use Flow\ETL\Column\Php\PhysicalFor;
use Flow\ETL\Column\Php\StructPhysical;
use Flow\ETL\Column\Php\TimePhysical;
use Flow\ETL\Column\Php\TimeZonePhysical;
use Flow\ETL\Column\Php\UuidPhysical;
use Flow\ETL\Column\Php\XmlDocumentPhysical;
use Flow\ETL\Column\Php\XmlElementPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use UnitEnum;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
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
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

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
        yield 'list' => [type_list(type_integer()), ListPhysical::class];
        yield 'map' => [type_map(type_string(), type_integer()), MapPhysical::class];
        yield 'structure' => [type_structure(['a' => type_integer()]), StructPhysical::class];
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
     * @param Type<mixed> $type
     * @param class-string $class
     */
    #[DataProvider('types')]
    public function test_maps_the_type_to_its_physical(Type $type, string $class): void
    {
        static::assertInstanceOf($class, (new PhysicalFor())->type($type));
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('refusals')]
    public function test_refuses(Type $type, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        (new PhysicalFor())->type($type);
    }

    public function test_structure_elements_are_keyed_by_name(): void
    {
        static::assertSame(
            ['a', 'b'],
            array_keys((new PhysicalFor())->elements(type_structure(['a' => type_integer(), 'b' => type_string()]))),
        );
    }
}
