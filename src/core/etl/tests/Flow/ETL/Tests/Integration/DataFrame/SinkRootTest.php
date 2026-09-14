<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Closure;
use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationError;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Loader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Pipeline\SinkFeed;
use Flow\ETL\Pipeline\TransactionalSinks;
use Flow\ETL\Sink;
use Flow\ETL\Sink\Transactional;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingScanExtractor;
use Flow\ETL\Tests\Double\RecordingSink;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\LimitTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_column;
use function array_sum;
use function file_get_contents;
use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\batch_size;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\drop;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\mask_columns;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_stream;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Types\DSL\type_integer;

final class SinkRootTest extends FlowIntegrationTestCase
{
    public function test_a_transformation_sink_with_add_row_index_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['name' => 'Alice', 'age' => 30],
                ['name' => 'Bob', 'age' => 25],
                ['name' => 'Charlie', 'age' => 35],
            ]))
            ->collect()
            ->write(to_transformation(add_row_index('row_num', StartFrom::ONE), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['name' => 'Alice', 'age' => 30, 'row_num' => 1],
                ['name' => 'Bob', 'age' => 25, 'row_num' => 2],
                ['name' => 'Charlie', 'age' => 35, 'row_num' => 3],
            ],
            $memory->dump(),
        );
    }

    public function test_a_transformation_sink_with_batch_size_transformation(): void
    {
        $loader = new SpyLoader();

        df()
            ->read(new FakeStaticOrdersExtractor(1000))
            ->collect()
            ->write(to_transformation(batch_size(500), $loader))
            ->run();

        static::assertSame(2, $loader->loadsCount);
    }

    public function test_a_transformation_sink_with_add_row_index_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $memory = new ArrayMemory();

        df()
            ->read(from_array($source))
            ->write(to_transformation(add_row_index('n', StartFrom::ONE), to_memory($memory)))
            ->run();

        static::assertSame([1, 2, 3, 4, 5, 6], array_column($memory->dump(), 'n'));
    }

    public function test_a_transformation_sink_with_batch_size_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $loader = new SpyLoader();

        df()
            ->read(from_array($source))
            ->write(to_transformation(batch_size(4), $loader))
            ->run();

        // The side pipeline is driven once over the whole stream, so batch_size(4) re-batches the stream instead of
        // each incoming batch - the same [4, 2] the outer frame's batchSize(4) produces.
        static::assertSame(2, $loader->loadsCount);
        static::assertSame([4, 2], $loader->loadedRowCounts());
    }

    public function test_a_transformation_sink_with_drop_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'password' => 'secret123'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'password' => 'secret456'],
            ]))
            ->write(to_transformation(drop('password', 'email'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $memory->dump(),
        );
    }

    public function test_a_transformation_sink_with_limit_transformer_does_not_stop_sibling_loaders(): void
    {
        $limited = new ArrayMemory();
        $sibling = new ArrayMemory();

        $source = [];

        for ($id = 1; $id <= 20; $id++) {
            $source[] = ['id' => $id];
        }

        df()
            ->read(from_array($source))
            ->load(to_transformation(new LimitTransformer(10), to_memory($limited)))
            ->load(to_memory($sibling))
            ->run();

        static::assertCount(10, $limited->dump());
        static::assertCount(20, $sibling->dump());
    }

    public function test_a_transformation_sink_with_limit_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
                ['id' => 4, 'name' => 'Diana'],
                ['id' => 5, 'name' => 'Eve'],
            ]))
            ->collect()
            ->write(to_transformation(limit(3), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
            ],
            $memory->dump(),
        );
    }

    public function test_a_transformation_sink_with_limit_transformation_across_batches(): void
    {
        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $memory = new ArrayMemory();

        df()
            ->read(from_array($source))
            ->write(to_transformation(limit(3), to_memory($memory)))
            ->run();

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $memory->dump());
    }

    public function test_a_transformation_sink_with_mask_columns_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'ssn' => '123-45-6789', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '987-65-4321', 'email' => 'bob@example.com'],
            ]))
            ->write(to_transformation(mask_columns(['ssn', 'email'], '***'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'ssn' => '***', 'email' => '***'],
                ['id' => 2, 'name' => 'Bob', 'ssn' => '***', 'email' => '***'],
            ],
            $memory->dump(),
        );
    }

    public function test_an_unresolved_column_inside_a_transformation_fails_when_the_plan_binds(): void
    {
        $sink = new SpyLoader();

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->write(to_transformation(select('nope'), $sink))
                ->run();

            static::fail('Expected the plan to refuse to bind the sink root against the prefix shape.');
        } catch (SchemaDefinitionNotFoundException $e) {
            static::assertSame('Schema definition for entry "nope" not found.', $e->getMessage());
        }

        static::assertSame(0, $sink->loadsCount);
    }

    public function test_a_nested_transformation_sink_applies_the_inner_limit_across_the_stream(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_sequence_number('id', 1, 12))
            ->batchSize(4)
            ->write(to_transformation(select('id'), to_transformation(limit(5), to_memory($memory))))
            ->run();

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]], $memory->dump());
    }

    public function test_a_nested_transformation_sink_keeps_row_index_continuous_across_batches(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_sequence_number('id', 1, 12))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_transformation(add_row_index('n', StartFrom::ONE), to_memory($memory)),
            ))
            ->run();

        static::assertSame([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], array_column($memory->dump(), 'n'));
    }

    public function test_a_three_level_nested_transformation_sink_delivers_the_whole_stream_and_closes_once(): void
    {
        $loader = new SpyLoader();

        df()
            ->read(from_sequence_number('id', 1, 12))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_transformation(select('id'), to_transformation(add_row_index('n', StartFrom::ONE), $loader)),
            ))
            ->run();

        static::assertSame(1, $loader->closureCount);
        static::assertSame([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], array_column($loader->loadedRowsToArray(), 'n'));
    }

    public function test_a_transformation_sink_with_select_transformation(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com', 'age' => 30],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com', 'age' => 25],
            ]))
            ->write(to_transformation(select('name', 'email'), to_memory($memory)))
            ->run();

        static::assertSame(
            [
                ['name' => 'Alice', 'email' => 'alice@example.com'],
                ['name' => 'Bob', 'email' => 'bob@example.com'],
            ],
            $memory->dump(),
        );
    }

    public function test_a_transformation_sink_with_stream_loader_across_batches(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_stream(
                    $path = $this->cacheDir->suffix('transformation_stream.txt')->path(),
                    output: Output::rows_count,
                ),
            ))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertSame("Rows: 4\nRows: 4\nRows: 4\n", $content);
    }

    public function test_a_sink_inside_a_joined_frame_runs_once_per_outer_run(): void
    {
        $inner = new SpyLoader();
        $outer = df()
            ->read(from_array([['id' => 1]]))
            ->join(df()->read(from_array([['id' => 1, 'v' => 'r']]))->write($inner), join_on(['id' => 'id'], 'r_'));

        $outer->run();
        $outer->run();

        static::assertSame([1, 1], $inner->loadedRowCounts());
        static::assertSame(2, $inner->closureCount);
    }

    public function test_a_sink_inside_a_frame_the_operator_never_pulls_never_runs(): void
    {
        $inner = new SpyLoader();

        df()
            ->read(from_array([], schema(int_schema('id'))))
            ->crossJoin(df()->read(from_array([['v' => 'r']]))->write($inner))
            ->run();

        static::assertSame(0, $inner->loadsCount);
        static::assertSame(0, $inner->closureCount);
    }

    public function test_a_later_filter_does_not_narrow_an_earlier_sink(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_text(__DIR__
            . '/Fixtures/Partitioning/multi_partition_pruning_test/year=*/month=*/day=*/*.txt')->partitionTypes(
                partition_types(year: type_integer()),
            ))
            ->write($spy)
            ->filter(ref('year')->equals(lit(2023)))
            ->run();

        static::assertSame(7, array_sum($spy->loadedRowCounts()));
    }

    public function test_a_later_limit_does_not_narrow_an_earlier_sink(): void
    {
        $spy = new SpyLoader();
        $extractor = new RecordingScanExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
        );

        $rows = df()->read($extractor)->write($spy)->limit(2)->fetch();

        static::assertCount(2, $rows);
        static::assertNull($extractor->scans[0]->limit);
        static::assertSame([3], $spy->loadedRowCounts());
    }

    public function test_two_writes_on_one_node_close_independently(): void
    {
        $failure = new RuntimeException('closure failed');
        $first = new ClosureThrowingLoader($failure);
        $second = new RecordingSink();

        try {
            df()
                ->read(from_array([['id' => 1]]))
                ->write($first)
                ->write($second)
                ->run();

            static::fail('Expected the first closure failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(1, $first->discarded);
        static::assertSame(1, $second->closed);
        static::assertSame(0, $second->discarded);
    }

    public function test_a_side_loader_failure_is_offered_once_against_the_users_loader(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $failure = new RuntimeException('boom');
        $loader = new ThrowingLoader($failure);

        df()
            ->read(from_array([['id' => 1]]))
            ->onError($handler)
            ->write(to_branch(lit(true), $loader))
            ->run();

        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(LoadingError::class, $error);
        static::assertSame($loader, $error->loader);
        static::assertSame($failure, $error->cause);
    }

    public function test_a_transaction_child_failure_is_offered_once_against_the_childs_feed(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $failure = new RuntimeException('boom');
        $transaction = new RecordingTransaction();

        df()
            ->read(from_array([['id' => 1]]))
            ->onError($handler)
            ->write(new Transactional($transaction, to_branch(lit(true), new ThrowingLoader($failure))))
            ->run();

        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(LoadingError::class, $error);
        static::assertInstanceOf(SinkFeed::class, $error->loader);
        static::assertSame($failure, $error->cause);
        static::assertSame(['begin', 'rollback', 'begin', 'commit'], $transaction->log);
    }

    /**
     * @param 'beginFailure'|'commitFailure' $failing
     * @param list<string> $log
     */
    #[DataProvider('failing_transaction_calls')]
    public function test_a_failing_transaction_call_is_offered_once_against_the_transaction_step(
        string $failing,
        array $log,
    ): void {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $failure = new RuntimeException('transaction failed');
        $transaction = new RecordingTransaction(...[$failing => $failure]);

        try {
            df()
                ->read(from_array([['id' => 1]]))
                ->onError($handler)
                ->write(new Transactional($transaction, new SpyLoader()))
                ->run();

            static::fail('Expected the drain transaction failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(LoadingError::class, $error);
        static::assertInstanceOf(TransactionalSinks::class, $error->loader);
        static::assertSame($failure, $error->cause);
        static::assertSame($log, $transaction->log);
    }

    public static function failing_transaction_calls(): Generator
    {
        yield 'begin' => ['beginFailure', ['begin', 'begin']];
        yield 'commit' => ['commitFailure', ['begin', 'commit', 'rollback', 'begin', 'commit', 'rollback']];
    }

    public function test_a_later_limit_still_drains_a_transaction_once(): void
    {
        $spy = new SpyLoader();
        $transaction = new RecordingTransaction();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write(new Transactional($transaction, to_branch(lit(true), $spy)))
            ->limit(1)
            ->run();

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame([1], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_node_shared_by_transaction_children_runs_once_per_row(): void
    {
        $first = new ArrayMemory();
        $second = new ArrayMemory();
        $transaction = new RecordingTransaction();

        df()
            ->read(from_sequence_number('id', 0, 5))
            ->batchSize(3)
            ->write(to_transformation(
                add_row_index('idx'),
                new Transactional($transaction, to_memory($first), to_memory($second)),
            ))
            ->run();

        static::assertSame([0, 1, 2, 3, 4, 5], array_column($first->dump(), 'idx'));
        static::assertSame([0, 1, 2, 3, 4, 5], array_column($second->dump(), 'idx'));
        static::assertSame(['begin', 'commit', 'begin', 'commit', 'begin', 'commit'], $transaction->log);
    }

    public function test_a_write_inside_a_transformation_sees_the_rows_its_outer_sink_sees(): void
    {
        $inner = new ArrayMemory();
        $outer = new ArrayMemory();

        df()
            ->read(from_sequence_number('id', 0, 5))
            ->batchSize(3)
            ->write(to_transformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->limit(
                4,
            )->write(to_memory($inner))), to_memory($outer)))
            ->run();

        static::assertSame([0, 1, 2, 3], array_column($inner->dump(), 'id'));
        static::assertSame([0, 1, 2, 3], array_column($outer->dump(), 'id'));
    }

    public function test_an_ending_failure_during_load_is_offered_once_against_the_feed(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $failure = new RuntimeException('closure failed');
        $loader = new ClosureThrowingLoader($failure);

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->onError($handler)
            ->write(to_transformation(limit(1), $loader))
            ->run();

        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(LoadingError::class, $error);
        static::assertInstanceOf(SinkFeed::class, $error->loader);
        static::assertSame($failure, $error->cause);
        static::assertSame(1, $loader->loadsCount);
    }

    public function test_a_closure_failure_is_never_offered_and_surfaces_the_users_exception(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $failure = new RuntimeException('closure failed');

        try {
            df()
                ->read(from_array([['id' => 1]]))
                ->onError($handler)
                ->write(to_branch(lit(true), new ClosureThrowingLoader($failure)))
                ->run();

            static::fail('Expected the closure failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame([], $handler->errors);
    }

    public function test_a_declined_failure_inside_a_sink_root_skips_only_that_batch(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->onError(ignore_error_handler())
            ->write(to_transformation(new ThrowWhenRowMatches('id', 1, new RuntimeException('boom')), $spy))
            ->run();

        static::assertSame([['id' => 2]], $spy->loadedRowsToArray());
        static::assertSame(1, $spy->closureCount);
    }

    public function test_a_declined_failure_keeps_loading_into_the_next_loader_like_a_plain_loader(): void
    {
        // skipBatch declines the failure inside the sink root only: the loader after it still receives every batch
        $tail = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->onError(ignore_error_handler())
            ->write(to_transformation(new ThrowWhenRowMatches('id', 2, new RuntimeException('boom')), new SpyLoader()))
            ->write($tail)
            ->run();

        static::assertSame([1, 2, 3], array_column($tail->loadedRowsToArray(), 'id'));
    }

    public function test_a_failed_run_does_not_close_the_sink(): void
    {
        $spy = new SpyLoader();
        $boom = new RuntimeException('boom');

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->write(to_transformation(new ThrowingTransformer($boom), $spy))
                ->run();

            static::fail('Expected the transformer failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame($boom, $e);
        }

        static::assertSame(0, $spy->closureCount);
        static::assertSame(0, $spy->loadsCount);
    }

    /**
     * @param Closure(SpyLoader): Sink $sinkOf
     */
    #[DataProvider('limited_sink_roots')]
    public function test_a_limit_inside_a_sink_root_ignores_later_batches_and_closes_once(Closure $sinkOf): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(1)
            ->write($sinkOf($spy))
            ->run();

        static::assertSame([1, 1], $spy->loadedRowCounts());
        static::assertSame(1, $spy->closureCount);
    }

    public static function limited_sink_roots(): Generator
    {
        yield 'transformation' => [static fn(SpyLoader $spy): Sink => to_transformation(limit(2), $spy)];
        yield 'branch' => [static fn(SpyLoader $spy): Sink => to_branch(lit(true), $spy)->withTransformation(limit(2))];
    }

    /**
     * @param Closure(Loader, Transformer): Sink $sinkOf
     */
    #[DataProvider('draining_sink_roots')]
    public function test_a_declined_drain_failure_skips_the_buffered_batch_and_closes_once(Closure $sinkOf): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([['id' => 1]]))
            ->onError(ignore_error_handler())
            ->write($sinkOf($spy, new ThrowingTransformer(new RuntimeException('boom'))))
            ->run();

        static::assertSame(0, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
    }

    /**
     * @param Closure(Loader, Transformer): Sink $sinkOf
     */
    #[DataProvider('draining_sink_roots')]
    public function test_a_drain_failure_surfaces_the_users_exception(Closure $sinkOf): void
    {
        $boom = new RuntimeException('boom');
        $loader = new ThrowingLoader($boom);

        try {
            df()
                ->read(from_array([['id' => 1]]))
                ->write($sinkOf($loader, new SpyTransformer()))
                ->run();

            static::fail('Expected the drain failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame($boom, $e);
        }

        static::assertSame(1, $loader->loadsCount);
    }

    /**
     * @param Closure(Loader, Transformer): Sink $sinkOf
     */
    #[DataProvider('draining_sink_roots')]
    public function test_a_drain_failure_is_offered_to_the_handler_once(Closure $sinkOf): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());

        df()
            ->read(from_array([['id' => 1]]))
            ->onError($handler)
            ->write($sinkOf(new SpyLoader(), new ThrowingTransformer(new RuntimeException('boom'))))
            ->run();

        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(TransformationError::class, $handler->errors[0]);
        static::assertSame('boom', $handler->errors[0]->cause->getMessage());
    }

    public static function draining_sink_roots(): Generator
    {
        yield 'transformation' => [
            static fn(Loader $sink, Transformer $after): Sink => to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->collect()->with($after),
            ), $sink),
        ];
        yield 'branch' => [
            static fn(Loader $sink, Transformer $after): Sink => to_branch(
                lit(true),
                $sink,
            )->withTransformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->collect()->with($after),
            )),
        ];
    }
}
