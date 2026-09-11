<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\TypeProjection;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class TypeProjectionTest extends FlowTestCase
{
    public function test_array_and_empty_array_are_both_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure([
            'a' => type_array(),
            'b' => type_list(type_empty_array()),
            'c' => type_map(type_string(), type_array()),
        ]);

        static::assertSame(
            'structure{a: json, b: list<json>, c: map<string, json>}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_array_leaf_is_projected_to_json(): void
    {
        static::assertEquals(type_json(), (new TypeProjection())->project(type_array()));
    }

    public function test_deeply_nested_array_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure([
            'a' => type_list(type_array()),
            'b' => type_map(type_string(), type_structure(['c' => type_array()])),
        ]);

        static::assertSame(
            'structure{a: list<json>, b: map<string, structure{c: json}>}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_deeply_nested_empty_array_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure([
            'a' => type_list(type_empty_array()),
            'b' => type_map(type_string(), type_structure(['c' => type_empty_array()])),
        ]);

        static::assertSame(
            'structure{a: list<json>, b: map<string, structure{c: json}>}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_empty_array_leaf_is_projected_to_json(): void
    {
        static::assertEquals(type_json(), (new TypeProjection())->project(type_empty_array()));
    }

    public function test_list_array_element_is_projected(): void
    {
        static::assertSame(
            'list<json>',
            (new TypeProjection())
                ->list(type_list(type_array()))
                ->toString(),
        );
    }

    public function test_list_element_is_projected(): void
    {
        static::assertSame(
            'list<json>',
            (new TypeProjection())
                ->list(type_list(type_empty_array()))
                ->toString(),
        );
    }

    public function test_list_without_empty_array_is_returned_untouched(): void
    {
        $list = type_list(type_integer());

        static::assertSame($list, (new TypeProjection())->list($list));
    }

    public function test_map_array_value_is_projected(): void
    {
        static::assertSame(
            'map<integer, json>',
            (new TypeProjection())->map(type_map(type_integer(), type_array()))->toString(),
        );
    }

    public function test_map_value_is_projected(): void
    {
        static::assertSame(
            'map<integer, json>',
            (new TypeProjection())->map(type_map(type_integer(), type_empty_array()))->toString(),
        );
    }

    public function test_map_without_empty_array_is_returned_untouched(): void
    {
        $map = type_map(type_integer(), type_string());

        static::assertSame($map, (new TypeProjection())->map($map));
    }

    public function test_optional_array_is_projected_to_optional_json(): void
    {
        static::assertSame(
            '?json',
            (new TypeProjection())
                ->project(type_optional(type_array()))
                ->toString(),
        );
    }

    public function test_optional_empty_array_is_projected_to_optional_json(): void
    {
        static::assertSame(
            '?json',
            (new TypeProjection())
                ->project(type_optional(type_empty_array()))
                ->toString(),
        );
    }

    public function test_optional_without_empty_array_is_returned_untouched(): void
    {
        $optional = type_optional(type_string());

        static::assertSame($optional, (new TypeProjection())->project($optional));
    }

    public function test_scalar_type_is_returned_untouched(): void
    {
        $string = type_string();

        static::assertSame($string, (new TypeProjection())->project($string));
    }

    public function test_structure_array_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure(['data' => type_array()]);

        static::assertSame(
            'structure{data: json}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_structure_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure(['data' => type_empty_array()]);

        static::assertSame(
            'structure{data: json}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_structure_optional_array_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure([
            'id' => type_integer(),
            'data' => structure_element('data', type_array(), optional: true),
        ]);

        static::assertSame(
            'structure{id: integer, data?: json}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_structure_optional_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure([
            'id' => type_integer(),
            'data' => structure_element('data', type_empty_array(), optional: true),
        ]);

        static::assertSame(
            'structure{id: integer, data?: json}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_interleaved_structure_keeps_every_position_and_flag(): void
    {
        static::assertSame(
            'structure{z: integer, a?: json, b: string}',
            (new TypeProjection())
                ->structure(new StructureType([
                    new StructureElement('z', type_integer()),
                    new StructureElement('a', type_array(), optional: true),
                    new StructureElement('b', type_string()),
                ]))
                ->toString(),
        );
    }

    public function test_structure_without_empty_array_is_returned_untouched(): void
    {
        /** @var StructureType<array<array-key, mixed>> $structure */
        $structure = type_structure(['id' => type_integer()]);

        static::assertSame($structure, (new TypeProjection())->structure($structure));
    }

    public function test_union_nested_in_a_structure_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure(['data' => type_union(type_list(type_string()), type_empty_array())]);

        static::assertSame(
            'structure{data: json|list<string>}',
            (new TypeProjection())
                ->structure($type)
                ->toString(),
        );
    }

    public function test_union_array_member_is_projected(): void
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_list(type_string()), type_array());

        static::assertSame(
            'json|list<string>',
            (new TypeProjection())
                ->union($union)
                ->toString(),
        );
    }

    public function test_union_member_is_projected(): void
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_list(type_string()), type_empty_array());

        static::assertSame(
            'json|list<string>',
            (new TypeProjection())
                ->union($union)
                ->toString(),
        );
    }

    public function test_union_without_empty_array_is_returned_untouched(): void
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_string(), type_integer());

        static::assertSame($union, (new TypeProjection())->union($union));
    }
}
