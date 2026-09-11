<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class DefinitionTest extends FlowTestCase
{
    public function test_compatibility_with_differences_in_metadat(): void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        static::assertTrue($definition->isCompatible(integer_schema('id', false, Metadata::fromArray([
            'description' => 'some_random_description',
        ]))));
    }

    public function test_compatibility_with_differences_in_nullability_that_is_a_backward_compatibility_break(): void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        static::assertFalse($definition->isCompatible(integer_schema('id', true, Metadata::fromArray([
            'description' => 'some_random_description',
        ]))));
    }

    public function test_equals_nullability(): void
    {
        $def = integer_schema('id', nullable: true);

        static::assertFalse($def->isSame(integer_schema('id', nullable: false)));
        static::assertTrue($def->isSame(integer_schema('id', nullable: true)));
    }

    public function test_equals_types(): void
    {
        $def = list_schema('list', type_list(type_integer()));

        static::assertTrue($def->isSame(list_schema('list', type_list(type_integer()))));
    }

    public function test_matches_when_type_matches(): void
    {
        static::assertTrue(integer_schema('test')->matches(1));
    }

    public function test_merge_definitions(): void
    {
        static::assertEquals(integer_schema('id', true), integer_schema('id')->merge(integer_schema('id', true)));
    }

    public function test_merge_nullable_with_non_nullable_dateime_definitions(): void
    {
        static::assertEquals(datetime_schema('col', true), datetime_schema('col')->merge(datetime_schema('col', true)));

        static::assertEquals(datetime_schema('col'), datetime_schema('col')->merge(datetime_schema('col')));
    }

    public function test_merging_anything_and_null(): void
    {
        static::assertEquals(integer_schema('id', true), integer_schema('id', false)->merge(null_schema('id')));
        static::assertEquals(float_schema('id', true), float_schema('id', false)->merge(null_schema('id')));
        static::assertEquals(bool_schema('id', true), bool_schema('id', false)->merge(null_schema('id')));
        static::assertEquals(datetime_schema('id', true), datetime_schema('id', false)->merge(null_schema('id')));
    }

    public function test_merging_anything_and_string(): void
    {
        static::assertEquals(string_schema('id', true), integer_schema('id', false)->merge(string_schema('id', true)));
        static::assertEquals(string_schema('id', true), float_schema('id', false)->merge(string_schema('id', true)));
        static::assertEquals(string_schema('id', true), bool_schema('id', false)->merge(string_schema('id', true)));
        static::assertEquals(string_schema('id', true), datetime_schema('id', false)->merge(string_schema('id', true)));
    }

    public function test_merging_date_with_datetime(): void
    {
        static::assertEquals(datetime_schema('datetime'), datetime_schema('datetime')->merge(date_schema('datetime')));
    }

    public function test_merging_different_entries(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, int and string');

        integer_schema('int')->merge(string_schema('string'));
    }

    public function test_merging_float_and_int_definition(): void
    {
        static::assertEquals(float_schema('id', false), float_schema('id')->merge(int_schema('id')));

        static::assertEquals(float_schema('id', true), float_schema('id')->merge(int_schema('id', true)));
    }

    public function test_merging_list_of_ints_and_floats(): void
    {
        static::assertEquals(
            list_schema('list', type_list(type_float())),
            list_schema('list', type_list(type_integer()))->merge(list_schema('list', type_list(type_float()))),
        );
    }

    public function test_merging_numeric_types(): void
    {
        static::assertEquals(float_schema('id', true), integer_schema('id', false)->merge(float_schema('id', true)));
        static::assertEquals(float_schema('id', true), float_schema('id', false)->merge(integer_schema('id', true)));
    }

    public function test_merging_time_with_date(): void
    {
        static::assertEquals(datetime_schema('datetime'), date_schema('datetime')->merge(time_schema('datetime')));
    }

    public function test_merging_time_with_datetime(): void
    {
        static::assertEquals(datetime_schema('datetime'), datetime_schema('datetime')->merge(time_schema('datetime')));
    }

    public function test_merging_two_null_definitions(): void
    {
        $merged = null_schema('id')->merge(null_schema('id'));

        static::assertInstanceOf(NullDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merging_two_different_lists(): void
    {
        static::assertEquals(
            list_schema('list', type_list(type_string())),
            list_schema('list', type_list(type_string()))->merge(list_schema('list', type_list(type_integer()))),
        );
    }

    public function test_merging_two_different_maps(): void
    {
        static::assertEquals(
            map_schema('map', type_map(type_string(), type_string())),
            map_schema('map', type_map(type_string(), type_string()))->merge(map_schema('map', type_map(
                type_string(),
                type_integer(),
            ))),
        );
    }

    public function test_merging_two_different_structures(): void
    {
        static::assertEquals(
            structure_schema('structure', type_structure([
                'street' => type_string(),
                'city' => type_string(),
            ])),
            structure_schema('structure', type_structure([
                'street' => type_string(),
                'city' => type_string(),
            ]))->merge(structure_schema('structure', type_structure([
                'street' => type_string(),
                'city' => type_integer(),
            ]))),
        );
    }

    public function test_merging_two_same_lists(): void
    {
        static::assertEquals(
            list_schema('list', type_list(type_integer())),
            list_schema('list', type_list(type_integer()))->merge(list_schema('list', type_list(type_integer()))),
        );
    }

    public function test_merging_two_same_maps(): void
    {
        static::assertEquals(
            map_schema('map', type_map(type_string(), type_string())),
            map_schema('map', type_map(type_string(), type_string()))->merge(map_schema('map', type_map(
                type_string(),
                type_string(),
            ))),
        );
    }

    public function test_normalize_and_from_array(): void
    {
        $definition = structure_schema(
            'structure',
            type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lng' => type_float(),
                ]),
            ]),
            false,
            Metadata::with('description', 'some_random_description')->add('priority', 1),
        );

        static::assertEquals($definition, definition_from_array($definition->normalize()));
    }

    public function test_not_matches_when_not_nullable_and_null_given(): void
    {
        static::assertFalse(integer_schema('test', false)->matches(null));
    }

    public function test_not_matches_when_type_does_not_match(): void
    {
        static::assertFalse(integer_schema('test')->matches('test'));
    }

    public function test_set_metadata(): void
    {
        $definition = integer_schema('id', metadata: Metadata::fromArray(['test' => 'test']));

        static::assertEquals(integer_schema('id', false, Metadata::fromArray([
            'description' => 'some_random_description',
        ])), $definition->setMetadata(Metadata::fromArray(['description' => 'some_random_description'])));
    }

    public function test_structure_definition_keeps_its_nested_type(): void
    {
        static::assertEquals(
            type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lng' => type_float(),
                ]),
            ]),
            structure_schema('address', type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lng' => type_float(),
                ]),
            ]))->type(),
        );
    }
}
