<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor\Described;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class DescribedTest extends FlowTestCase
{
    public function test_root_and_schema_are_the_values_it_was_built_with(): void
    {
        $root = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());
        $schema = schema(int_schema('id'));

        $plan = new Described($root, $schema);

        static::assertSame($root, $plan->root());
        static::assertSame($schema, $plan->schema);
        static::assertSame($schema, $plan->schema());
    }
}
