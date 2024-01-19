<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Function\ListSelect;
use PHPUnit\Framework\TestCase;

final class ListSelectTest extends TestCase
{
    public function test_selecting_non_existing_value_from_list_using_alias() : void
    {
        $list = row(
            list_entry(
                'list',
                [
                    ['id' => 1, 'name' => 'test'],
                    ['id' => 2, 'name' => 'test2'],
                    ['id' => 3, 'name' => 'test3'],
                ],
                type_list(struct_type([
                    struct_element('id', type_int()),
                    struct_element('name', type_string()),
                ]))
            )
        );

        $this->assertEquals(
            [
                ['id' => 1, 'mail' => null],
                ['id' => 2, 'mail' => null],
                ['id' => 3, 'mail' => null],
            ],
            (new ListSelect(ref('list'), ref('id'), ref('mail')))->eval($list)
        );
    }

    public function test_selecting_value_from_list() : void
    {
        $list = row(
            list_entry(
                'list',
                [
                    ['id' => 1, 'name' => 'test'],
                    ['id' => 2, 'name' => 'test2'],
                    ['id' => 3, 'name' => 'test3'],
                ],
                type_list(struct_type([
                    struct_element('id', type_int()),
                    struct_element('name', type_string()),
                ]))
            )
        );

        $this->assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            (new ListSelect(ref('list'), 'id'))->eval($list)
        );
    }

    public function test_selecting_value_from_list_using_alias() : void
    {
        $list = row(
            list_entry(
                'list',
                [
                    ['id' => 1, 'name' => 'test'],
                    ['id' => 2, 'name' => 'test2'],
                    ['id' => 3, 'name' => 'test3'],
                ],
                type_list(struct_type([
                    struct_element('id', type_int()),
                    struct_element('name', type_string()),
                ]))
            )
        );

        $this->assertEquals(
            [
                ['new_id' => 1],
                ['new_id' => 2],
                ['new_id' => 3],
            ],
            (new ListSelect(ref('list'), ref('id')->as('new_id')))->eval($list)
        );
    }

    public function test_selecting_value_from_simple_list() : void
    {
        $list = row(
            list_entry(
                'list',
                [
                    'a', 'b', 'c', 'd',
                ],
                type_list(type_string())
            )
        );

        $this->assertEquals(
            [
                ['id' => null],
                ['id' => null],
                ['id' => null],
                ['id' => null],
            ],
            (new ListSelect(ref('list'), ref('id')))->eval($list)
        );
    }
}
