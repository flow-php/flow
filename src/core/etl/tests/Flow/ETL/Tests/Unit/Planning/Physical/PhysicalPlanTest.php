<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PipelinesMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PhysicalPlanTest extends FlowTestCase
{
    public function test_a_described_plan_returns_the_values_it_was_built_with(): void
    {
        $root = PipelinesMother::root(from_array([['id' => 1]]), []);
        $schema = schema(int_schema('id'));

        $plan = new PhysicalPlan($root, $schema);

        static::assertSame($root, $plan->root());
        static::assertSame($schema, $plan->schema());
        static::assertNull($plan->refusal());
        static::assertTrue($plan->described());
    }

    public function test_a_refused_plan_throws_its_refusal_for_the_schema(): void
    {
        $why = DataDependentSchemaException::step('JoinEachRowsTransformer', 'x');
        $plan = new PhysicalPlan(PipelinesMother::root(from_array([['id' => 1]]), []), null, $why);

        static::assertSame($why, $plan->refusal());
        static::assertFalse($plan->described());

        try {
            $plan->schema();

            static::fail('Expected the refusal to be thrown.');
        } catch (DataDependentSchemaException $e) {
            static::assertSame($why, $e);
        }
    }

    public function test_a_refusal_from_elsewhere_keeps_the_returned_rows_schema(): void
    {
        $plan = new PhysicalPlan(
            PipelinesMother::root(from_array([['id' => 1]]), []),
            schema(int_schema('id')),
            DataDependentSchemaException::step('JoinEachRowsTransformer', 'x'),
        );

        static::assertEquals(schema(int_schema('id')), $plan->schema());
        static::assertFalse($plan->described());
    }

    public function test_a_plan_without_a_schema_or_a_refusal_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A plan without a schema requires a refusal');

        new PhysicalPlan(PipelinesMother::root(from_array([['id' => 1]]), []), null);
    }

    public function test_a_plan_counts_only_when_built_so(): void
    {
        $root = PipelinesMother::root(from_array([['id' => 1]]), []);
        $why = DataDependentSchemaException::step('JoinEachRowsTransformer', 'x');
        $plan = new PhysicalPlan($root, null, $why, true);

        static::assertTrue($plan->countsOnly());
        static::assertSame([$root], $plan->pipelines());
        static::assertSame($root, $plan->root());
        static::assertSame($why, $plan->refusal());
    }
}
