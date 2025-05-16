<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{list_entry, ref, row, struct_entry};
use function Flow\Types\DSL\{type_integer, type_optional};
use function Flow\Types\DSL\{type_list, type_string, type_structure};
use Flow\ETL\Function\StructureSelect;
use Flow\ETL\Tests\FlowTestCase;

final class StructureSelectTest extends FlowTestCase
{
    public function test_selecting_multiple_values_from_structure() : void
    {
        $structure = struct_entry(
            'struct',
            [
                'id' => 1,
                'name' => 'test',
            ],
            type_structure([
                'id' => type_integer(),
                'name' => type_string(),
            ])
        );

        self::assertEquals(
            ['id' => 1, 'name' => 'test'],
            (new StructureSelect(ref('struct'), ref('id'), ref('name')))
                ->eval(row($structure))
        );
    }

    public function test_selecting_single_value_from_structure() : void
    {
        $structure = struct_entry(
            'struct',
            [
                'id' => 1,
                'name' => 'test',
            ],
            type_structure([
                'id' => type_integer(),
                'name' => type_string(),
            ])
        );

        self::assertEquals(
            ['id' => 1],
            (new StructureSelect(ref('struct'), 'id'))
                ->eval(row($structure))
        );
    }

    public function test_selecting_single_value_from_structure_with_alias() : void
    {
        $structure = struct_entry(
            'struct',
            [
                'id' => 1,
                'name' => 'test',
            ],
            type_structure([
                'id' => type_integer(),
                'name' => type_string(),
            ])
        );

        self::assertEquals(
            ['new_id' => 1],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))
                ->eval(row($structure))
        );
    }

    public function test_selecting_values_from_empty_structure() : void
    {
        $structure = struct_entry(
            'struct',
            [
                'id' => null,
                'email' => 'email@email.com',
                'name' => null,
            ],
            type_structure([
                'id' => type_optional(type_integer()),
                'email' => type_string(),
                'name' => type_optional(type_string()),
            ])
        );

        self::assertEquals(
            ['new_id' => null],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))
                ->eval(row($structure))
        );
    }

    public function test_selecting_values_from_list() : void
    {
        $list = list_entry(
            'list',
            [
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
            ],
            type_list(
                type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
                ])
            )
        );

        self::assertNull(
            (new StructureSelect(ref('list'), ref('id')))->eval(row($list))
        );
    }
}
