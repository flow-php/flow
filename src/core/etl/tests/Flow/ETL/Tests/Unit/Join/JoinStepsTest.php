<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSteps;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class JoinStepsTest extends FlowTestCase
{
    public function test_builds_a_hash_join_processor(): void
    {
        $steps = JoinSteps::of(
            PhysicalPlanMother::reading(from_rows(rows(schema()))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            config_builder()->build(),
        );

        static::assertCount(1, $steps);
        static::assertInstanceOf(HashJoinProcessor::class, $steps[0]);
    }

    public function test_of_takes_a_frame_output_as_its_right_side(): void
    {
        $right = PhysicalPlanMother::reading(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))));

        $steps = JoinSteps::of($right, Expression::on(['id' => 'id'], 'r_'), Join::inner, config_builder()->build());

        static::assertCount(1, $steps);
        static::assertInstanceOf(HashJoinProcessor::class, $steps[0]);
        static::assertSame(
            ['id', 'r_id'],
            $steps[0]
                ->bind(schema(int_schema('id')))
                ->output
                ->references()
                ->names(),
        );
    }
}
