<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\RetryStrategy;

use Flow\ETL\Retry\RetryStrategy\AnyThrowable;
use PHPUnit\Framework\TestCase;

final class AnyThrowableTest extends TestCase
{
    public function test_respects_max_attempts() : void
    {
        $strategy = new AnyThrowable(3);
        $exception = new \RuntimeException('test');

        self::assertTrue($strategy->shouldRetry($exception, 1));
        self::assertTrue($strategy->shouldRetry($exception, 2));
        self::assertTrue($strategy->shouldRetry($exception, 3));
        self::assertFalse($strategy->shouldRetry($exception, 4));
        self::assertFalse($strategy->shouldRetry($exception, 100));
    }

    public function test_retries_on_any_exception() : void
    {
        $strategy = new AnyThrowable(5);

        self::assertTrue($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \RuntimeException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \InvalidArgumentException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \LogicException('test'), 2));
        self::assertTrue($strategy->shouldRetry(new \Error('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \TypeError('test'), 1));
    }

    public function test_throws_exception_for_negative_limit() : void
    {
        $this->expectException(\Flow\ETL\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Retry limit must be greater than 0');

        new AnyThrowable(-1);
    }

    public function test_throws_exception_for_zero_limit() : void
    {
        $this->expectException(\Flow\ETL\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Retry limit must be greater than 0');

        new AnyThrowable(0);
    }

    public function test_works_with_custom_exceptions() : void
    {
        $customException = new class('test') extends \Exception {};
        $anotherException = new class('test') extends \Error {};

        $strategy = new AnyThrowable(3);

        self::assertTrue($strategy->shouldRetry($customException, 1));
        self::assertTrue($strategy->shouldRetry($anotherException, 1));
    }
}
