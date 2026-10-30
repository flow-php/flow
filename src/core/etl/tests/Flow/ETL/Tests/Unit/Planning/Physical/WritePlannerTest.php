<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Loader\MemoryLoader;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\WritePlannerMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;

final class WritePlannerTest extends FlowTestCase
{
    public function test_a_bare_write_on_a_spine_node_is_that_nodes_loader_and_takes_no_id(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());
        $writes = new Writes(new Write($read, $loader));
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        static::assertSame([$loader], $planner->attach($writes, $onSpine)[$read]);
    }

    public function test_two_writes_sharing_a_two_node_path_off_the_spine_are_fed_through_one_side_pipeline(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::limit(NodeMother::select($read), 5);
        $writes = new Writes(
            new Write($shared, to_memory(new ArrayMemory())),
            new Write($shared, to_memory(new ArrayMemory())),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        static::assertSame(
            [BranchPipeline::class],
            array_map(static fn($step) => $step::class, $planner->attach($writes, $onSpine)[$read]),
        );
    }

    public function test_two_writes_sharing_only_part_of_their_path_nest_a_second_side_pipeline(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $writes = new Writes(
            new Write($shared, to_memory(new ArrayMemory())),
            new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory())),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        static::assertSame(
            [BranchPipeline::class],
            array_map(static fn($step) => $step::class, $planner->attach($writes, $onSpine)[$read]),
        );
    }

    public function test_two_writes_whose_paths_part_below_the_shared_node_each_get_their_own_pipeline_under_it(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $writes = new Writes(
            new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory())),
            new Write(NodeMother::limit($shared, 3), to_memory(new ArrayMemory())),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        static::assertSame(
            [BranchPipeline::class],
            array_map(static fn($step) => $step::class, $planner->attach($writes, $onSpine)[$read]),
        );
    }

    public function test_a_transaction_is_one_step_over_its_children(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $writes = new Writes(
            new Transaction(
                new RecordingTransaction(),
                new Write($read, to_memory(new ArrayMemory())),
                new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
            ),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        static::assertSame(
            [TransactionGroup::class],
            array_map(static fn($step) => $step::class, $planner->attach($writes, $onSpine)[$read]),
        );
    }

    public function test_a_write_outside_a_transaction_sharing_a_node_with_one_of_its_children_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $writes = new Writes(
            new Transaction(
                new RecordingTransaction(),
                new Write($shared, to_memory(new ArrayMemory())),
                new Write(NodeMother::limit($read, 1), to_memory(new ArrayMemory())),
            ),
            new Write($shared, to_memory(new ArrayMemory())),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A write outside a transaction cannot share a node with one of its children');

        $planner->attach($writes, $onSpine);
    }

    public function test_the_children_of_one_transaction_must_attach_to_the_same_node(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);
        $writes = new Writes(
            new Transaction(
                new RecordingTransaction(),
                new Write($read, to_memory(new ArrayMemory())),
                new Write($select, to_memory(new ArrayMemory())),
            ),
        );
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read, $select);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Every write of one transaction must attach to the same node');

        $planner->attach($writes, $onSpine);
    }

    public function test_a_write_sharing_no_node_with_the_spine_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $writes = new Writes(new Write(NodeMother::read(), to_memory(new ArrayMemory())));
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A write root shares no node with the plan: ' . MemoryLoader::class);

        $planner->attach($writes, $onSpine);
    }

    /**
     * @return Generator<string, array{Node, Writes, int}>
     */
    public static function attached_writes(): Generator
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $sharedPath = NodeMother::limit(NodeMother::select($read), 5);

        yield 'a bare write on a spine node takes no id' => [
            $read,
            new Writes(new Write($read, to_memory(new ArrayMemory()))),
            0,
        ];
        yield 'two writes sharing a two node path share one pipeline' => [
            $read,
            new Writes(
                new Write($sharedPath, to_memory(new ArrayMemory())),
                new Write($sharedPath, to_memory(new ArrayMemory())),
            ),
            1,
        ];
        yield 'two writes sharing part of their path nest a second pipeline' => [
            $read,
            new Writes(
                new Write($shared, to_memory(new ArrayMemory())),
                new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory())),
            ),
            2,
        ];
        yield 'two writes parting below the shared node get a pipeline each' => [
            $read,
            new Writes(
                new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory())),
                new Write(NodeMother::limit($shared, 3), to_memory(new ArrayMemory())),
            ),
            3,
        ];
    }

    #[DataProvider('attached_writes')]
    public function test_attached_writes_take_the_ids_before_the_spine(Node $read, Writes $writes, int $next): void
    {
        [$planner, $onSpine] = WritePlannerMother::over($writes, $read);
        $planner->attach($writes, $onSpine);

        static::assertSame($next, $planner->next());
    }
}
