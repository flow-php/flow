<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Function\StructureSelect;
use PHPUnit\Framework\TestCase;

final class StructureSelectTest extends TestCase
{
    public function test_selecting_multiple_values_from_structure() : void
    {
        $structure = struct_entry(
            'struct',
            [
                'id' => 1,
                'name' => 'test',
            ],
            struct_type([
                struct_element('id', type_int()),
                struct_element('name', type_string()),
            ])
        );

        $this->assertEquals(
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
            struct_type([
                struct_element('id', type_int()),
                struct_element('name', type_string()),
            ])
        );

        $this->assertEquals(
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
            struct_type([
                struct_element('id', type_int()),
                struct_element('name', type_string()),
            ])
        );

        $this->assertEquals(
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
                'email' => 'email@email.com',
            ],
            struct_type([
                struct_element('id', type_int(true)),
                struct_element('email', type_string()),
                struct_element('name', type_string(true)),
            ])
        );

        $this->assertEquals(
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
                struct_type([
                    struct_element('id', type_int()),
                    struct_element('name', type_string()),
                ])
            )
        );

        $this->assertNull(
            (new StructureSelect(ref('list'), ref('id')))->eval(row($list))
        );
    }
}
