<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Schema\Definition\TypeProjection;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;

use function Flow\ETL\DSL\ref;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
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
            (new TypeProjection(ref('col')))
                ->structure($type)
                ->toString(),
        );
    }

    public function test_array_leaf_is_projected_to_json(): void
    {
        static::assertEquals(type_json(), (new TypeProjection(ref('col')))->project(type_array()));
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
            (new TypeProjection(ref('col')))
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
            (new TypeProjection(ref('col')))
                ->structure($type)
                ->toString(),
        );
    }

    public function test_empty_array_leaf_is_projected_to_json(): void
    {
        static::assertEquals(type_json(), (new TypeProjection(ref('col')))->project(type_empty_array()));
    }

    public function test_list_array_element_is_projected(): void
    {
        static::assertSame(
            'list<json>',
            (new TypeProjection(ref('col')))
                ->list(type_list(type_array()))
                ->toString(),
        );
    }

    public function test_list_element_is_projected(): void
    {
        static::assertSame(
            'list<json>',
            (new TypeProjection(ref('col')))
                ->list(type_list(type_empty_array()))
                ->toString(),
        );
    }

    public function test_list_without_empty_array_is_returned_untouched(): void
    {
        $list = type_list(type_integer());

        static::assertSame($list, (new TypeProjection(ref('col')))->list($list));
    }

    public function test_map_array_value_is_projected(): void
    {
        static::assertSame(
            'map<integer, json>',
            (new TypeProjection(ref('col')))->map(type_map(type_integer(), type_array()))->toString(),
        );
    }

    public function test_map_value_is_projected(): void
    {
        static::assertSame(
            'map<integer, json>',
            (new TypeProjection(ref('col')))->map(type_map(type_integer(), type_empty_array()))->toString(),
        );
    }

    public function test_map_without_empty_array_is_returned_untouched(): void
    {
        $map = type_map(type_integer(), type_string());

        static::assertSame($map, (new TypeProjection(ref('col')))->map($map));
    }

    public function test_optional_array_is_projected_to_optional_json(): void
    {
        static::assertSame(
            '?json',
            (new TypeProjection(ref('col')))
                ->project(type_optional(type_array()))
                ->toString(),
        );
    }

    public function test_optional_empty_array_is_projected_to_optional_json(): void
    {
        static::assertSame(
            '?json',
            (new TypeProjection(ref('col')))
                ->project(type_optional(type_empty_array()))
                ->toString(),
        );
    }

    public function test_optional_without_empty_array_is_returned_untouched(): void
    {
        $optional = type_optional(type_string());

        static::assertSame($optional, (new TypeProjection(ref('col')))->project($optional));
    }

    public function test_scalar_type_is_returned_untouched(): void
    {
        $string = type_string();

        static::assertSame($string, (new TypeProjection(ref('col')))->project($string));
    }

    public function test_structure_array_element_is_projected(): void
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $type = type_structure(['data' => type_array()]);

        static::assertSame(
            'structure{data: json}',
            (new TypeProjection(ref('col')))
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
            (new TypeProjection(ref('col')))
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
            (new TypeProjection(ref('col')))
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
            (new TypeProjection(ref('col')))
                ->structure($type)
                ->toString(),
        );
    }

    public function test_interleaved_structure_keeps_every_position_and_flag(): void
    {
        static::assertSame(
            'structure{z: integer, a?: json, b: string}',
            (new TypeProjection(ref('col')))
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

        static::assertSame($structure, (new TypeProjection(ref('col')))->structure($structure));
    }

    public function test_an_optional_union_list_element_becomes_optional(): void
    {
        static::assertSame(
            'list<?integer>',
            (new TypeProjection(ref('col')))
                ->list(type_list(type_union(type_integer(), type_null())))
                ->toString(),
        );
    }

    public function test_an_optional_union_map_value_becomes_optional(): void
    {
        static::assertSame(
            'map<string, ?integer>',
            (new TypeProjection(ref('col')))->map(type_map(type_string(), type_union(
                type_integer(),
                type_null(),
            )))->toString(),
        );
    }

    public function test_an_optional_union_structure_element_becomes_optional(): void
    {
        static::assertSame(
            'structure{x: ?integer}',
            (new TypeProjection(ref('col')))
                ->structure(type_structure(['x' => type_union(type_integer(), type_null())]))
                ->toString(),
        );
    }

    public function test_an_optional_union_member_is_projected(): void
    {
        static::assertSame(
            'list<?json>',
            (new TypeProjection(ref('col')))
                ->list(type_list(type_union(type_array(), type_null())))
                ->toString(),
        );
    }

    public function test_a_multi_member_union_element_is_refused(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);
        $this->expectExceptionMessage(
            'Column "col" cannot hold elements of type "integer|string": a column holds exactly one type, and so '
            . 'does every element inside it.'
            . "\n"
            . 'Only "null|T" is a valid union - that is a nullable element.'
            . "\n"
            . 'Possible fixes:'
            . "\n"
            . '* Declare the widest common element type, e.g. type_string()'
            . "\n"
            . '* Declare json_schema(\'col\') when the shape is genuinely dynamic',
        );

        (new TypeProjection(ref('col')))->list(type_list(type_union(type_integer(), type_string())));
    }

    public function test_a_union_nested_deeper_is_refused(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);
        $this->expectExceptionMessage('Column "col" cannot hold elements of type "integer|string"');

        (new TypeProjection(ref('col')))->structure(type_structure([
            'x' => type_list(type_union(type_integer(), type_string())),
        ]));
    }
}
