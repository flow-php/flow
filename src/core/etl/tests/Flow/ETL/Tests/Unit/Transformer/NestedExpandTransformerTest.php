<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Transformer\NestedExpandTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class NestedExpandTransformerTest extends FlowTestCase
{
    public function test_emits_one_row_per_element_and_none_for_an_empty_list(): void
    {
        $step = (new ScalarFunctionTransformer('s', structure(['tag' => ref('tags')->expand()])))->bind(
            ListColumnsMother::tagsSchema(),
        )->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'x']],
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => ['tag' => 'y']],
            ],
            $step->transform(
                rows(
                    ListColumnsMother::tagsSchema(),
                    row(['id' => 'a', 'tags' => ['x', 'y']]),
                    row(['id' => 'b', 'tags' => []]),
                ),
                flow_context(config()),
            )->toArray(),
        );
    }

    public function test_unpack_emits_one_row_per_element(): void
    {
        $step = (new ScalarFunctionTransformer(
            'u',
            structure(['tag' => ref('tags')->expand()])->unpack(schema(str_schema('tag'))),
        ))->bind(ListColumnsMother::tagsSchema())->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'x'],
                ['id' => 'a', 'tags' => ['x', 'y'], 'u.tag' => 'y'],
            ],
            $step->transform(
                rows(ListColumnsMother::tagsSchema(), row(['id' => 'a', 'tags' => ['x', 'y']])),
                flow_context(config()),
            )->toArray(),
        );
    }

    public function test_bind_rebinds_against_another_input(): void
    {
        $step = (new ScalarFunctionTransformer('s', structure(['tag' => ref('tags')->expand()])))->bind(
            ListColumnsMother::tagsSchema(),
        )->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);

        $rebound = $step->bind(schema(list_schema('tags', type_list(type_integer()))));

        static::assertInstanceOf(NestedExpandTransformer::class, $rebound->step);
        static::assertEquals(
            schema(
                list_schema('tags', type_list(type_integer())),
                structure_schema('s', type_structure(['tag' => type_integer()])),
            ),
            $rebound->output,
        );
    }

    public function test_a_definition_entry_is_declared_as_given(): void
    {
        $bound = (new ScalarFunctionTransformer(
            str_schema('s', nullable: true),
            concat(ref('id'), ref('tags')->expand()),
        ))->bind(ListColumnsMother::tagsSchema());
        $step = $bound->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);
        static::assertEquals(
            schema(str_schema('id'), list_schema('tags', type_list(type_string())), str_schema('s', nullable: true)),
            $bound->output,
        );
        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => 'ax'],
                ['id' => 'a', 'tags' => ['x', 'y'], 's' => 'ay'],
            ],
            $step->transform(
                rows(ListColumnsMother::tagsSchema(), row(['id' => 'a', 'tags' => ['x', 'y']])),
                flow_context(config()),
            )->toArray(),
        );
    }

    public function test_a_batch_under_another_schema_than_the_bound_one_is_conformed(): void
    {
        $step = (new ScalarFunctionTransformer('s', structure(['tag' => ref('tags')->expand()])))->bind(schema(
            str_schema('id'),
            list_schema('tags', type_list(type_string())),
            str_schema('note', nullable: true),
        ))->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);

        $result = $step->transform(
            rows(ListColumnsMother::tagsSchema(), row(['id' => 'a', 'tags' => ['x']])),
            flow_context(config()),
        );

        static::assertEquals(
            schema(
                str_schema('id'),
                list_schema('tags', type_list(type_string())),
                str_schema('note', nullable: true),
                structure_schema('s', type_structure(['tag' => type_string()])),
            ),
            $result->schema(),
        );
        static::assertSame([['id' => 'a', 'tags' => ['x'], 'note' => null, 's' => ['tag' => 'x']]], $result->toArray());
    }

    public function test_a_null_under_a_not_null_definition_names_its_output_row(): void
    {
        $input = schema(list_schema('tags', type_list(type_optional(type_string()))));
        $step = (new ScalarFunctionTransformer(str_schema('s'), ref('tags')->expand()->coalesce()))->bind($input)->step;

        static::assertInstanceOf(NestedExpandTransformer::class, $step);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "s" (row 1)');

        $step->transform(rows($input, row(['tags' => ['x']]), row(['tags' => [null]])), flow_context(config()));
    }
}
