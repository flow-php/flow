<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Plan\Raw;
use Flow\ETL\Plan\Refusal;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;

final class RawTest extends FlowTestCase
{
    public function test_root_and_refusal_are_the_values_it_was_built_with(): void
    {
        $root = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());
        $why = Refusal::of(DataDependentSchemaException::step('JoinEachRowsTransformer', 'x'));

        $plan = new Raw($root, $why);

        static::assertSame($root, $plan->root());
        static::assertSame($why, $plan->why);
    }
}
