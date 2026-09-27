<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Executor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CrossJoinRowsTransformerTest extends FlowTestCase
{
    public function test_bind_concatenates_the_left_and_the_right_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new CrossJoinRowsTransformer(
                PhysicalPlanMother::reading(from_rows(array_to_rows([[
                    'name' => 'Alice',
                ]], schema(str_schema('name'))))),
                new Executor(),
            ))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_bind_prefixes_every_right_side_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('right_name')),
            (new CrossJoinRowsTransformer(
                PhysicalPlanMother::reading(from_rows(array_to_rows([[
                    'name' => 'Alice',
                ]], schema(str_schema('name'))))),
                new Executor(),
                'right_',
            ))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_the_right_side_is_fetched_through_a_frame_output(): void
    {
        $transformer = new CrossJoinRowsTransformer(
            PhysicalPlanMother::reading(from_rows(array_to_rows([
                ['name' => 'Alice'],
                ['name' => 'Bob'],
            ], schema(str_schema('name'))))),
            new Executor(),
            'r_',
        );

        static::assertSame(
            [['id' => 1, 'r_name' => 'Alice'], ['id' => 1, 'r_name' => 'Bob']],
            $transformer->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context())->toArray(),
        );
    }
}
