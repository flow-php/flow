<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\ListSelect;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final class ListSelectTest extends FlowTestCase
{
    public function test_selecting_from_a_list_of_maps_declares_optional_map_value_types(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            new ListSelect(ref('list'), 'field'),
            schema(list_schema('list', type_list(type_map(type_string(), type_integer())))),
        );

        static::assertSame('?list<structure{field: ?integer}>', $resolved->returns()->toString());
    }

    public function test_selecting_from_a_list_of_maps_of_mixed_keeps_the_value_mixed(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            new ListSelect(ref('list'), 'field'),
            schema(list_schema('list', type_list(type_map(type_string(), type_mixed())))),
        );

        static::assertSame('?list<structure{field: mixed}>', $resolved->returns()->toString());
    }

    public function test_selecting_from_a_list_of_scalars_declares_null_element_types(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            new ListSelect(ref('list'), 'field'),
            schema(list_schema('list', type_list(type_string()))),
        );

        static::assertSame('?list<structure{field: null}>', $resolved->returns()->toString());
    }

    public function test_selecting_non_existing_value_from_list_using_alias(): void
    {
        $list = row([
            'list' => [
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
                ['id' => 3, 'name' => 'test3'],
            ],
        ]);

        static::assertEquals(
            [
                ['id' => 1, 'mail' => null],
                ['id' => 2, 'mail' => null],
                ['id' => 3, 'mail' => null],
            ],
            (new ListSelect(ref('list'), ref('id'), ref('mail')))->eval($list, flow_context()),
        );
    }

    public function test_selecting_value_from_list(): void
    {
        $list = row([
            'list' => [
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
                ['id' => 3, 'name' => 'test3'],
            ],
        ]);

        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            (new ListSelect(ref('list'), 'id'))->eval($list, flow_context()),
        );
    }

    public function test_selecting_value_from_list_using_alias(): void
    {
        $list = row([
            'list' => [
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
                ['id' => 3, 'name' => 'test3'],
            ],
        ]);

        static::assertEquals(
            [
                ['new_id' => 1],
                ['new_id' => 2],
                ['new_id' => 3],
            ],
            (new ListSelect(ref('list'), ref('id')->as('new_id')))->eval($list, flow_context()),
        );
    }

    public function test_selecting_value_from_simple_list(): void
    {
        $list = row(['list' => [
            'a',
            'b',
            'c',
            'd',
        ]]);

        static::assertEquals(
            [
                ['id' => null],
                ['id' => null],
                ['id' => null],
                ['id' => null],
            ],
            (new ListSelect(ref('list'), ref('id')))->eval($list, flow_context()),
        );
    }
}
