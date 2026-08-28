<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\DSL;

use DateTimeZone;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Row\Entry\TimeZoneEntry;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\String\StringTypeNarrower;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\time_zone_entry;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;

final class DefinitionFromTypeTest extends FlowTestCase
{
    public function test_an_optional_type_yields_a_nullable_definition(): void
    {
        $definition = definition_from_type('c', type_optional(type_integer()));

        static::assertInstanceOf(IntegerDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
    }

    public function test_a_nested_optional_list_keeps_its_element_optionality(): void
    {
        $definition = definition_from_type('c', type_optional(type_list(type_optional(type_integer()))));

        static::assertInstanceOf(ListDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
        static::assertTrue(type_equals(type_list(type_optional(type_integer())), $definition->type()));
    }

    public function test_an_optional_null_type_yields_a_null_definition(): void
    {
        static::assertInstanceOf(NullDefinition::class, definition_from_type('c', type_optional(type_null())));
    }

    public function test_definition_from_type_refuses_an_empty_array_type(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Column "tags" cannot be typed as array{} - an empty array is a value, not a column type. '
            . 'Declare the element type, e.g. type_list(type_string()).',
        );

        definition_from_type('tags', type_empty_array());
    }

    public function test_definition_from_type_names_the_column_it_was_given_as_a_reference(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column "labels" cannot be typed as array{}');

        definition_from_type(ref('labels'), type_empty_array());
    }

    public function test_definition_from_type_reduces_an_optional_union_to_a_nullable_column(): void
    {
        $definition = definition_from_type('value', type_union(type_string(), type_null()));

        static::assertInstanceOf(StringDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
    }

    public function test_definition_from_type_refuses_a_non_optional_union(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);
        $this->expectExceptionMessage(
            'Column "value" cannot be typed as "integer|string": a column holds exactly one type.'
            . "\n"
            . 'Only "null|T" is a valid union - that is a nullable column.'
            . "\n"
            . 'Possible fixes:'
            . "\n"
            . "* Declare the widest common type: str_schema('value')\n"
            . "* Declare json_schema('value') when the shape is genuinely dynamic",
        );

        definition_from_type('value', type_union(type_string(), type_integer()));
    }

    public function test_union_schema_delegates_the_same_refusal(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);

        // @mago-expect analysis:deprecated-function
        union_schema('value', type_union(type_string(), type_integer()));
    }

    /**
     * The string inference ladder narrows an IANA identifier to type_time_zone(), and
     * definition_from_type() had no arm for it, so the ladder crashed on its own output.
     */
    public function test_definition_from_type_builds_a_timezone_definition(): void
    {
        static::assertInstanceOf(TimeZoneDefinition::class, definition_from_type('tz', type_time_zone()));
    }

    public function test_the_string_narrower_output_can_always_be_typed(): void
    {
        foreach (['+02:00', 'Europe/Warsaw', 'UTC'] as $value) {
            static::assertInstanceOf(TimeZoneDefinition::class, definition_from_type(
                'tz',
                (new StringTypeNarrower())->narrow($value),
            ));
        }
    }

    public function test_time_zone_entry_and_time_zone_schema_delegate_purely(): void
    {
        static::assertEquals(new TimeZoneEntry('tz', new DateTimeZone('UTC')), time_zone_entry('tz', 'UTC'));
        static::assertEquals(
            new TimeZoneEntry('tz', new DateTimeZone('UTC')),
            time_zone_entry('tz', new DateTimeZone('UTC')),
        );
        static::assertEquals(new TimeZoneEntry('tz', null), time_zone_entry('tz', null));
        static::assertEquals(new TimeZoneDefinition('tz', true), time_zone_schema('tz', true));
    }

    public function test_union_schema_delegates_an_optional_union_to_a_nullable_column(): void
    {
        // @mago-expect analysis:deprecated-function
        $definition = union_schema('value', type_union(type_string(), type_null()));

        static::assertInstanceOf(StringDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
    }
}
