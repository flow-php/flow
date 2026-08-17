<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\FailedRetryException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\ArrayLoader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Retry\RetryStrategy\OnExceptionTypes;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Time\FakeSleep;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use LogicException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\delay_fixed;
use function Flow\ETL\DSL\duration_milliseconds;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\retry_any_throwable;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\write_with_retries;

final class RetryLoaderTest extends TestCase
{
    public function test_a_branch_without_transformation_can_be_retried(): void
    {
        $flaky = new class() implements Loader {
            /** @var array<array<mixed>> */
            public array $loadedRows = [];

            public int $loads = 0;

            public function load(Rows $rows, FlowContext $context): void
            {
                if (++$this->loads === 1) {
                    throw new RuntimeException('Simulated transient failure on the first attempt');
                }

                $this->loadedRows[] = $rows->toArray();
            }
        };
        $sleep = new FakeSleep();

        write_with_retries(loader: to_branch(lit(true), $flaky), sleep: $sleep)->load(
            rows(row(int_entry('id', 1))),
            flow_context(config()),
        );

        static::assertSame([[['id' => 1]]], $flaky->loadedRows);
        static::assertSame(1, $sleep->sleepCount());
    }

    public function test_a_raw_transformer_wrap_is_still_refused(): void
    {
        $spy = new SpyLoader();
        $sleep = new FakeSleep();
        $retry = write_with_retries(
            loader: to_transformation(new ScalarFunctionFilterTransformer(lit(true)), $spy),
            sleep: $sleep,
        );

        try {
            $retry->load(rows(row(int_entry('id', 1))), flow_context(config()));

            static::fail('Expected the raw-Transformer wrap to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('RetryLoader cannot wrap this loader', $e->getMessage());
        }

        static::assertSame(0, $sleep->sleepCount());
        static::assertSame(0, $spy->loadsCount);
    }

    public function test_a_transformation_armed_after_construction_is_refused_at_load(): void
    {
        $spy = new SpyLoader();
        $branch = to_branch(lit(true), $spy);
        $sleep = new FakeSleep();
        $retry = write_with_retries(loader: $branch, sleep: $sleep);

        $retry->load(rows(row(int_entry('id', 1))), $context = flow_context(config()));
        $branch->withTransformation(new CallbackTransformation(
            static fn(DataFrame $dataFrame): DataFrame => $dataFrame->collect(),
        ));

        try {
            $retry->load(rows(row(int_entry('id', 2))), $context);

            static::fail('Expected the armed transformation to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('RetryLoader cannot wrap this loader', $e->getMessage());
        }

        // Never entered the retry loop, so this is not a FailedRetryException path.
        static::assertSame(0, $sleep->sleepCount());
        static::assertSame(1, $spy->loadsCount);
    }

    public function test_a_transformation_wrapped_loader_is_refused_at_load(): void
    {
        $spy = new SpyLoader();
        $sleep = new FakeSleep();
        $retry = write_with_retries(loader: to_transformation(select('id'), $spy), sleep: $sleep);

        try {
            $retry->load(rows(row(int_entry('id', 1))), flow_context(config()));

            static::fail('Expected the transformation-wrapped loader to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('RetryLoader cannot wrap this loader', $e->getMessage());
        }

        static::assertSame(0, $sleep->sleepCount());
        static::assertSame(0, $spy->loadsCount);
    }

    public function test_a_transformation_wrapped_loader_nested_below_a_replay_safe_wrapper_is_refused(): void
    {
        $spy = new SpyLoader();
        $sleep = new FakeSleep();
        $retry = write_with_retries(loader: to_branch(lit(true), to_transformation(select('id'), $spy)), sleep: $sleep);

        try {
            $retry->load(rows(row(int_entry('id', 1))), flow_context(config()));

            static::fail('Expected the nested transformation-wrapped loader to be refused.');
        } catch (InvalidLogicException $e) {
            static::assertStringContainsString('RetryLoader cannot wrap this loader', $e->getMessage());
        }

        static::assertSame(0, $sleep->sleepCount());
        static::assertSame(0, $spy->loadsCount);
    }

    public function test_closure_is_a_no_op_for_a_loader_that_is_not_closure_aware(): void
    {
        $output = [];

        write_with_retries(new ArrayLoader($output))->closure(flow_context(config()));

        static::assertSame([], $output);
    }

    public function test_closure_is_forwarded_to_the_wrapped_loader(): void
    {
        $context = flow_context(config());
        $spy = new SpyLoader();
        $loader = write_with_retries($spy);

        $loader->load(rows(row(int_entry('id', 1))), $context);
        $loader->closure($context);

        static::assertSame(1, $spy->loadsCount);
        static::assertSame(1, $spy->closureCount);
        static::assertSame([$context], $spy->closureContexts);
    }

    public function test_closure_is_not_covered_by_the_retry_strategy(): void
    {
        $failingLoader = new class() implements Closure, Loader {
            public int $closureCount = 0;

            public function closure(FlowContext $context): void
            {
                $this->closureCount++;

                throw new RuntimeException('Commit failed');
            }

            public function load(Rows $rows, FlowContext $context): void {}
        };

        $sleep = new FakeSleep();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Commit failed');

        try {
            write_with_retries(loader: $failingLoader, sleep: $sleep)->closure(flow_context(config()));
        } finally {
            static::assertSame(1, $failingLoader->closureCount);
            static::assertSame(0, $sleep->sleepCount());
        }
    }

    public function test_exposing_the_wrapped_loader(): void
    {
        $spy = new SpyLoader();

        static::assertSame([$spy], write_with_retries($spy)->loaders());
    }

    public function test_exhausting_all_retries(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $exception = new RuntimeException('Persistent error');
        $mockLoader
            ->expects(self::exactly(4)) // 1 initial + 3 retries
            ->method('load')
            ->with($rows, $context)
            ->willThrowException($exception);

        $retryLoader = write_with_retries(
            $mockLoader,
            retry_any_throwable(3),
            delay_fixed(duration_milliseconds(100)),
            $sleep,
        );

        $this->expectException(FailedRetryException::class);
        $this->expectExceptionMessage('Retry failed after 4 attempts.');

        $retryLoader->load($rows, $context);

        static::assertSame(3, $sleep->sleepCount());
        static::assertSame(300, $sleep->totalMilliseconds());
    }

    public function test_retry_loader_does_not_create_duplicates_retries_same_rows(): void
    {
        $mockLoader = new class() implements Loader {
            /** @var array<array<mixed>> */
            public array $loadedRows = [];

            public int $loads = 0;

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->loads++;

                if ($this->loads === 2) {
                    throw new RuntimeException('Simulated transient failure on attempt 2');
                }

                $this->loadedRows[] = $rows->toArray();
            }
        };

        $context = flow_context(config());
        $sleep = new FakeSleep();

        $retryLoader = write_with_retries(loader: $mockLoader, sleep: $sleep);

        $retryLoader->load(rows(row(int_entry('id', 1))), $context);
        $retryLoader->load(rows(row(int_entry('id', 2))), $context);
        $retryLoader->load(rows(row(int_entry('id', 3))), $context);

        static::assertCount(3, $mockLoader->loadedRows);
        static::assertEquals(
            [
                [['id' => 1]],
                [['id' => 2]],
                [['id' => 3]],
            ],
            $mockLoader->loadedRows,
        );
        static::assertSame(4, $mockLoader->loads);
    }

    public function test_retry_on_transient_failure_that_succeeds(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $callCount = 0;
        $mockLoader
            ->expects(self::exactly(2))
            ->method('load')
            ->with($rows, $context)
            ->willReturnCallback(function () use (&$callCount): void {
                $callCount++;

                if ($callCount === 1) {
                    throw new RuntimeException('Transient error');
                }
            });

        $retryLoader = write_with_retries(
            $mockLoader,
            retry_any_throwable(3),
            delay_fixed(duration_milliseconds(100)),
            $sleep,
        );

        $retryLoader->load($rows, $context);

        static::assertSame(1, $sleep->sleepCount());
        static::assertSame(100, $sleep->totalMilliseconds());
    }

    public function test_retry_strategy_determining_not_to_retry(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $exception = new LogicException('Logic error');
        $mockLoader->expects(self::once())->method('load')->with($rows, $context)->willThrowException($exception);

        $retryLoader = write_with_retries(
            $mockLoader,
            new OnExceptionTypes([RuntimeException::class], 3),
            delay_fixed(duration_milliseconds(100)),
            $sleep,
        );

        $this->expectException(FailedRetryException::class);
        $this->expectExceptionMessage('Retry failed after 1 attempts.');

        $retryLoader->load($rows, $context);

        static::assertSame(0, $sleep->sleepCount());
    }

    public function test_successful_load_without_retries(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());

        $mockLoader->expects(self::once())->method('load')->with($rows, $context);

        $retryLoader = write_with_retries($mockLoader, retry_any_throwable(3), delay_fixed(duration_milliseconds(100)));

        $retryLoader->load($rows, $context);
    }
}
