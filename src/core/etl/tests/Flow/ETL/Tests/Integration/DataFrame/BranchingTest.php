<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_array, lit, ref, to_branch, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

final class BranchingTest extends FlowIntegrationTestCase
{
    public function test_branching() : void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'group' => 'A'],
                ['id' => 2, 'group' => 'B'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 5, 'group' => 'A'],
                ['id' => 6, 'group' => 'C'],
            ]))
            ->write(
                to_branch(
                    ref('group')->equals(lit('A')),
                    to_memory($memoryA = new ArrayMemory()),
                )
            )
            ->write(
                to_branch(
                    ref('group')->isIn(lit(['B', 'C'])),
                    to_memory($memoryBC = new ArrayMemory()),
                )
            )
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'group' => 'A'],
                ['id' => 3, 'group' => 'A'],
                ['id' => 5, 'group' => 'A'],
            ],
            $memoryA->dump(),
        );
        self::assertSame(
            [
                ['id' => 2, 'group' => 'B'],
                ['id' => 4, 'group' => 'B'],
                ['id' => 6, 'group' => 'C'],
            ],
            $memoryBC->dump(),
        );
    }
}
