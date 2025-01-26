<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_array, lit, ref, to_branch, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\{DataFrame, Transformation};

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

    public function test_branching_with_transformation() : void
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
                )->withTransformation(new class implements Transformation {
                    public function transform(DataFrame $dataFrame) : DataFrame
                    {
                        return $dataFrame->withEntry('group_name', lit('A'));
                    }
                })
            )
            ->write(
                to_branch(
                    ref('group')->isIn(lit(['B', 'C'])),
                    to_memory($memoryBC = new ArrayMemory()),
                )->withTransformation(new class implements Transformation {
                    public function transform(DataFrame $dataFrame) : DataFrame
                    {
                        return $dataFrame->withEntry('group_name', lit('BC'));
                    }
                })
            )
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'group' => 'A', 'group_name' => 'A'],
                ['id' => 3, 'group' => 'A', 'group_name' => 'A'],
                ['id' => 5, 'group' => 'A', 'group_name' => 'A'],
            ],
            $memoryA->dump(),
        );
        self::assertSame(
            [
                ['id' => 2, 'group' => 'B', 'group_name' => 'BC'],
                ['id' => 4, 'group' => 'B', 'group_name' => 'BC'],
                ['id' => 6, 'group' => 'C', 'group_name' => 'BC'],
            ],
            $memoryBC->dump(),
        );
    }
}
