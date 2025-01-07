<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{list_entry, ref, row, type_int, type_list, type_string, type_structure};
use Flow\ETL\Function\ListSelect;
use Flow\ETL\Tests\FlowTestCase;

final class ListSelectTest extends FlowTestCase
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
                type_list(type_structure([
                    'id' => type_int(),
                    'name' => type_string(),
                ]))
            )
        );

        self::assertEquals(
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
                type_list(type_structure([
                    'id' => type_int(),
                    'name' => type_string(),
                ]))
            )
        );

        self::assertEquals(
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
                type_list(type_structure([
                    'id' => type_int(),
                    'name' => type_string(),
                ]))
            )
        );

        self::assertEquals(
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

        self::assertEquals(
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
