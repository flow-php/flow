<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\StructureSelect;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\struct_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureSelectTest extends FlowTestCase
{
    public function test_selecting_multiple_values_from_structure(): void
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
            ]),
        );

        static::assertEquals(
            ['id' => 1, 'name' => 'test'],
            (new StructureSelect(ref('struct'), ref('id'), ref('name')))->eval(row($structure), flow_context()),
        );
    }

    public function test_selecting_single_value_from_structure(): void
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
            ]),
        );

        static::assertEquals(
            ['id' => 1],
            (new StructureSelect(ref('struct'), 'id'))->eval(row($structure), flow_context()),
        );
    }

    public function test_selecting_single_value_from_structure_with_alias(): void
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
            ]),
        );

        static::assertEquals(
            ['new_id' => 1],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))->eval(row($structure), flow_context()),
        );
    }

    public function test_selecting_values_from_empty_structure(): void
    {
        $structure = struct_entry(
            'struct',
            // @mago-ignore analysis:possibly-invalid-argument
            [
                'id' => null,
                'email' => 'email@email.com',
                'name' => null,
            ],
            type_structure([
                'id' => type_optional(type_integer()),
                'email' => type_string(),
                'name' => type_optional(type_string()),
            ]),
        );

        static::assertEquals(
            ['new_id' => null],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))->eval(row($structure), flow_context()),
        );
    }

    public function test_selecting_values_from_list(): void
    {
        $list = list_entry(
            'list',
            [
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
            ],
            type_list(type_structure([
                'id' => type_integer(),
                'name' => type_string(),
            ])),
        );

        static::assertNull((new StructureSelect(ref('list'), ref('id')))->eval(row($list), flow_context()));
    }
}
