<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\flow_context;
use Flow\ETL\Exception\FailedRetryException;
use Flow\ETL\Loader;
use Flow\ETL\Retry\RetryStrategy\OnExceptionTypes;
use Flow\ETL\Time\FakeSleep;
use PHPUnit\Framework\TestCase;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\write_with_retries;
use function Flow\ETL\DSL\retry_any_throwable;
use function Flow\ETL\DSL\duration_milliseconds;
use function Flow\ETL\DSL\delay_fixed;

final class RetryLoaderTest extends TestCase
{
    public function test_successful_load_without_retries(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());

        $mockLoader->expects($this->once())
            ->method('load')
            ->with($rows, $context);

        $retryLoader = write_with_retries(
            $mockLoader,
            retry_any_throwable(3),
            delay_fixed(duration_milliseconds(100))
        );

        $retryLoader->load($rows, $context);
    }

    public function test_retry_on_transient_failure_that_succeeds(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $callCount = 0;
        $mockLoader->expects($this->exactly(2))
            ->method('load')
            ->with($rows, $context)
            ->willReturnCallback(function () use (&$callCount) {
                $callCount++;
                if ($callCount === 1) {
                    throw new \RuntimeException('Transient error');
                }
            });

        $retryLoader = write_with_retries(
            $mockLoader,
            retry_any_throwable(3),
            delay_fixed(duration_milliseconds(100)),
            $sleep
        );

        $retryLoader->load($rows, $context);

        self::assertSame(1, $sleep->sleepCount());
        self::assertSame(100, $sleep->totalMilliseconds());
    }

    public function test_exhausting_all_retries(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $exception = new \RuntimeException('Persistent error');
        $mockLoader->expects($this->exactly(4)) // 1 initial + 3 retries
            ->method('load')
            ->with($rows, $context)
            ->willThrowException($exception);

        $retryLoader = write_with_retries(
            $mockLoader,
            retry_any_throwable(3),
            delay_fixed(duration_milliseconds(100)),
            $sleep
        );

        $this->expectException(FailedRetryException::class);
        $this->expectExceptionMessage('Retry failed after 4 attempts.');

        $retryLoader->load($rows, $context);

        self::assertSame(3, $sleep->sleepCount());
        self::assertSame(300, $sleep->totalMilliseconds());
    }

    public function test_retry_strategy_determining_not_to_retry(): void
    {
        $mockLoader = $this->createMock(Loader::class);
        $rows = rows();
        $context = flow_context(config());
        $sleep = new FakeSleep();

        $exception = new \LogicException('Logic error');
        $mockLoader->expects($this->once())
            ->method('load')
            ->with($rows, $context)
            ->willThrowException($exception);

        $retryLoader = write_with_retries(
            $mockLoader,
            new OnExceptionTypes([\RuntimeException::class], 3),
            delay_fixed(duration_milliseconds(100)),
            $sleep
        );

        $this->expectException(FailedRetryException::class);
        $this->expectExceptionMessage('Retry failed after 1 attempts.');

        $retryLoader->load($rows, $context);

        self::assertSame(0, $sleep->sleepCount());
    }

    public function test_retry_loader_does_not_create_duplicates_retries_same_rows(): void
    {
        $mockLoader = new class() implements Loader {
            /** @var array<array<mixed>>  */
            public array $loadedRows = [];
            public int $loads = 0;

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->loads++;

                if ($this->loads === 2) {
                    throw new \RuntimeException('Simulated transient failure on attempt 2');
                }

                $this->loadedRows[] = $rows->toArray();
            }
        };

        $context = flow_context(config());
        $sleep = new FakeSleep();

        $retryLoader = write_with_retries(
            loader: $mockLoader,
            sleep: $sleep
        );

        $retryLoader->load(rows(row(int_entry('id', 1))), $context);
        $retryLoader->load(rows(row(int_entry('id', 2))), $context);
        $retryLoader->load(rows(row(int_entry('id', 3))), $context);

        self::assertCount(3, $mockLoader->loadedRows);
        self::assertEquals(
            [
                [['id' => 1]],
                [['id' => 2]],
                [['id' => 3]],
            ],
            $mockLoader->loadedRows
        );
        self::assertSame(4, $mockLoader->loads);
    }
}