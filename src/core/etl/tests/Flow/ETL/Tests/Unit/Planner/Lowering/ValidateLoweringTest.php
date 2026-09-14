<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Plan\Node\Validate;
use Flow\ETL\Planner\Lowering\ValidateLowering;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ValidateLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SchemaValidationLoader::class],
            array_map(
                static fn($step) => $step::class,
                (new ValidateLowering())->steps(
                    new Validate(NodeMother::read(), schema(int_schema('id')), new StrictValidator()),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
