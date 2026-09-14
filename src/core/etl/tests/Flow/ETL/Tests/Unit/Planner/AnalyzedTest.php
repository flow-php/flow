<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Planner\Analyzed;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\LimitTransformer;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class AnalyzedTest extends FlowTestCase
{
    public function test_steps_bound_schema_and_nested_are_the_values_it_was_built_with(): void
    {
        $steps = [new LimitTransformer(5)];
        $bound = [new LimitTransformer(5)];
        $schema = schema(int_schema('id'));
        $nested = new Described(
            new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context()),
            $schema,
        );

        $analyzed = new Analyzed($steps, $bound, $schema, $nested);

        static::assertSame($steps, $analyzed->steps);
        static::assertSame($bound, $analyzed->bound);
        static::assertSame($schema, $analyzed->schema);
        static::assertSame($nested, $analyzed->nested);
    }

    public function test_nested_defaults_to_null(): void
    {
        static::assertNull((new Analyzed([], [], null))->nested);
    }

    public function test_nested_or_fail_returns_the_nested_plan(): void
    {
        $nested = new Described(
            new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context()),
            schema(),
        );

        static::assertSame($nested, (new Analyzed([], [], null, $nested))->nestedOrFail());
    }

    public function test_nested_or_fail_throws_when_there_is_no_nested_plan(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A Frame child always has a nested plan');

        (new Analyzed([], [], null))->nestedOrFail();
    }
}
