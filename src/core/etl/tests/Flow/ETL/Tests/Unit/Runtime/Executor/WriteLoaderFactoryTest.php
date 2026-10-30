<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Planning\Logical\Node\Transform;
use Flow\ETL\Planning\Physical\PlannedNodes;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Planning\Physical\WritePlanner;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\Executor\TransactionLoader;
use Flow\ETL\Runtime\Executor\WriteLoaderFactory;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Values\Schema;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class WriteLoaderFactoryTest extends FlowTestCase
{
    public function test_rows_fed_to_the_branch_reach_its_write(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);
        $planned = new PlannedNodes();
        $context->config->planner()->node($select, $planned);
        $spy = new SpyLoader();

        $feed = (new WriteLoaderFactory())->feed(
            (new WritePlanner($planned))->branch([$select], [$spy], $read),
            $context->errorHandler(),
            $context,
        );
        $feed->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $feed->closure();

        static::assertSame([2], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_host_without_a_schema_feeds_an_empty_schema(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(new UndescribableRowLessExtractor());
        $planned = new PlannedNodes();
        $context->config->planner()->node($read, $planned);
        $spy = new SpyLoader();
        $branch = (new WritePlanner($planned))->branch([], [$spy], $read);

        $feed = (new WriteLoaderFactory())->feed($branch, $context->errorHandler(), $context);
        $feed->load(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $feed->closure();

        static::assertNotNull($planned->refusal());
        static::assertEquals(new Schema(), $branch->input);
        static::assertSame([1], $spy->loadedRowCounts());
    }

    public function test_a_throw_only_handler_surfaces_a_failing_step(): void
    {
        $context = NodeMother::context()->withErrorHandler(new IgnoreError());
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $transform = new Transform($read, new ThrowingTransformer(new RuntimeException('boom')));
        $planned = new PlannedNodes();
        $context->config->planner()->node($transform, $planned);

        $feed = (new WriteLoaderFactory())->feed(
            (new WritePlanner($planned))->branch([$transform], [new SpyLoader()], $read),
            new ThrowError(),
            $context,
        );

        $this->expectExceptionObject(new RuntimeException('boom'));

        $feed->load(array_to_rows([['id' => 1]], schema(int_schema('id'))));
    }

    public function test_a_branch_inside_a_branch_runs_as_the_feed_of_its_parents_pipeline(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $planned = new PlannedNodes();
        $context->config->planner()->node($read, $planned);
        $planner = new WritePlanner($planned);
        $spy = new SpyLoader();
        $inner = $planner->branch([], [$spy], $read);
        $outer = $planner->branch([], [$inner], $read);

        $feed = (new WriteLoaderFactory())->feed($outer, $context->errorHandler(), $context);
        $feed->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $feed->closure();

        static::assertSame([2], $spy->loadedRowCounts());
    }

    public function test_a_transaction_group_runs_as_one_transaction_over_its_writes(): void
    {
        $context = NodeMother::context();
        $transaction = new RecordingTransaction();
        $spy = new SpyLoader();

        $loader = (new WriteLoaderFactory())->transactional(new TransactionGroup($transaction, [$spy]), $context);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $loader->closure();

        static::assertInstanceOf(TransactionLoader::class, $loader);
        static::assertSame([1], $spy->loadedRowCounts());
        static::assertNotSame([], $transaction->log);
    }

    public function test_a_branch_is_built_once_per_factory(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $planned = new PlannedNodes();
        $context->config->planner()->node($read, $planned);
        $branch = (new WritePlanner($planned))->branch([], [new SpyLoader()], $read);
        $writes = new WriteLoaderFactory();

        static::assertSame(
            $writes->step($branch, $context->errorHandler(), $context),
            $writes->step($branch, $context->errorHandler(), $context),
        );
        static::assertNotSame(
            $writes->step($branch, $context->errorHandler(), $context),
            (new WriteLoaderFactory())->step($branch, $context->errorHandler(), $context),
        );
    }

    public function test_a_transaction_is_built_once_per_factory(): void
    {
        $context = NodeMother::context();
        $group = new TransactionGroup(new RecordingTransaction(), [new SpyLoader()]);
        $writes = new WriteLoaderFactory();

        static::assertSame(
            $writes->step($group, $context->errorHandler(), $context),
            $writes->step($group, $context->errorHandler(), $context),
        );
        static::assertNotSame(
            $writes->step($group, $context->errorHandler(), $context),
            (new WriteLoaderFactory())->step($group, $context->errorHandler(), $context),
        );
    }

    public function test_a_loader_write_is_itself(): void
    {
        $spy = new SpyLoader();

        static::assertSame($spy, (new WriteLoaderFactory())->write(
            $spy,
            NodeMother::context()->errorHandler(),
            NodeMother::context(),
        ));
    }

    public function test_a_step_that_is_no_branch_is_itself(): void
    {
        $limit = new LimitTransformer(1);

        static::assertSame($limit, (new WriteLoaderFactory())->step(
            $limit,
            NodeMother::context()->errorHandler(),
            NodeMother::context(),
        ));
    }
}
