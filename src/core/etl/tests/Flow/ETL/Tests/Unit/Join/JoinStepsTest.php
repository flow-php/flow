<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSteps;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;

final class JoinStepsTest extends FlowTestCase
{
    public function test_builds_a_hash_join_processor(): void
    {
        $steps = JoinSteps::of(
            df()->read(from_rows(rows())),
            Expression::on(['id' => 'user_id']),
            Join::left,
            config_builder()->build(),
        );

        static::assertCount(1, $steps);
        static::assertInstanceOf(HashJoinProcessor::class, $steps[0]);
    }
}
