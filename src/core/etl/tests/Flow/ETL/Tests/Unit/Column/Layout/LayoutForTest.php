<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\BooleanLayout;
use Flow\ETL\Column\Layout\FixedBinary16Layout;
use Flow\ETL\Column\Layout\Float64Layout;
use Flow\ETL\Column\Layout\Int32Layout;
use Flow\ETL\Column\Layout\Int64Layout;
use Flow\ETL\Column\Layout\LayoutFor;
use Flow\ETL\Column\Layout\Utf8Layout;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class LayoutForTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, class-string}>
     */
    public static function types(): Generator
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
    #[DataProvider('types')]
    public function test_maps_the_type_to_its_layout(Type $type, string $class): void
    {
        static::assertInstanceOf($class, (new LayoutFor())->type($type));
    }

    public function test_a_container_has_no_scalar_layout(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('type list<integer> has no scalar layout');

        (new LayoutFor())->type(type_list(type_integer()));
    }
}
