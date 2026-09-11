<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\list_ref;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ListSelectTest extends FlowTestCase
{
    public function test_selection_order_and_optional_flags_are_preserved(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            list_ref('list')->select('b', 'a'),
            schema(list_schema(
                'list',
                type_list(type_structure([
                    'a' => type_integer(),
                    'b' => structure_element('b', type_string(), optional: true),
                    'c' => type_integer(),
                ])),
            )),
        );

        static::assertSame('?list<structure{b?: string, a: integer}>', $resolved->returns()->toString());
    }

    public function test_selecting_a_numeric_element_name_by_its_string_form(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            list_ref('list')->select('0'),
            schema(list_schema('list', type_list(type_structure([0 => type_integer(), 'b' => type_string()])))),
        );

        static::assertSame('?list<structure{0: integer}>', $resolved->returns()->toString());
    }

    public function test_selecting_properties_from_list(): void
    {
        $rows = df()
            ->read(from_array([
                [
                    'list' => [
                        ['id' => 1, 'name' => 'test'],
                        ['id' => 2, 'name' => 'test2'],
                        ['id' => 3, 'name' => 'test3'],
                    ],
                ],
                [
                    'list' => [
                        ['id' => 4, 'name' => 'test4'],
                        ['id' => 5, 'name' => 'test5'],
                        ['id' => 6, 'name' => 'test6'],
                    ],
                ],
            ]))
            ->withEntry('list', list_ref('list')->select('id'))
            ->fetch();

        static::assertEquals(
            [
                ['list' => [['id' => 1], ['id' => 2], ['id' => 3]]],
                ['list' => [['id' => 4], ['id' => 5], ['id' => 6]]],
            ],
            $rows->toArray(),
        );
    }
}
