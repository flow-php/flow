<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Planner\PlannedNode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\LimitTransformer;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PlannedNodeTest extends FlowTestCase
{
    public function test_steps_bound_and_schema_are_the_values_it_was_built_with(): void
    {
        $steps = [new LimitTransformer(5)];
        $bound = [new LimitTransformer(5)];
        $schema = schema(int_schema('id'));

        $planned = new PlannedNode($steps, $bound, $schema);

        static::assertSame($steps, $planned->steps);
        static::assertSame($bound, $planned->bound);
        static::assertSame($schema, $planned->schema);
    }
}
