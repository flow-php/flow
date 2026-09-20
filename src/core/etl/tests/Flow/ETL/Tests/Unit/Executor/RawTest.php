<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Raw;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class RawTest extends FlowTestCase
{
    public function test_root_and_refusal_are_the_values_it_was_built_with(): void
    {
        $root = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());
        $why = DataDependentSchemaException::step('JoinEachRowsTransformer', 'x');

        $plan = new Raw($root, $why);

        static::assertSame($root, $plan->root());
        static::assertSame($why, $plan->why);
    }

    public function test_schema_throws_the_refusal_when_the_returned_rows_are_not_described(): void
    {
        $why = DataDependentSchemaException::step('JoinEachRowsTransformer', 'x');
        $plan = new Raw(new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context()), $why);

        try {
            $plan->schema();

            static::fail('Expected the refusal to be thrown.');
        } catch (DataDependentSchemaException $e) {
            static::assertSame($why, $e);
        }
    }

    public function test_schema_is_the_returned_rows_schema_when_the_refusal_came_from_elsewhere(): void
    {
        $plan = new Raw(
            new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context()),
            DataDependentSchemaException::step('JoinEachRowsTransformer', 'x'),
            schema(int_schema('id')),
        );

        static::assertEquals(schema(int_schema('id')), $plan->schema());
    }
}
