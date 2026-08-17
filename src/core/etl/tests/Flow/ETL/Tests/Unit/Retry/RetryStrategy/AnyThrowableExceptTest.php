<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\RetryStrategy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Retry\RetryStrategy\AnyThrowableExcept;
use Flow\ETL\Tests\FlowTestCase;
use OutOfBoundsException;
use RuntimeException;
use Throwable;
use TypeError;

final class AnyThrowableExceptTest extends FlowTestCase
{
    public function test_empty_array_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Exception types cannot be empty');

        new AnyThrowableExcept([], 3);
    }

    public function test_error_types_are_supported(): void
    {
        $strategy = new AnyThrowableExcept([TypeError::class], 3);

        static::assertFalse($strategy->shouldRetry(new TypeError('boom'), 1));
        static::assertTrue($strategy->shouldRetry(new RuntimeException('boom'), 1));
    }

    public function test_excluding_throwable_interface_retries_nothing(): void
    {
        $strategy = new AnyThrowableExcept([Throwable::class], 3);

        static::assertFalse($strategy->shouldRetry(new RuntimeException('boom'), 1));
    }

    public function test_invalid_class_name_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Class 'Flow\\NotAClass' does not exist");

        // @mago-ignore analysis:possibly-invalid-argument
        new AnyThrowableExcept(['Flow\NotAClass'], 3);
    }

    public function test_non_throwable_class_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is not a Throwable');

        // @mago-ignore analysis:invalid-argument
        new AnyThrowableExcept([self::class], 3);
    }

    public function test_respects_max_attempts(): void
    {
        $strategy = new AnyThrowableExcept([InvalidLogicException::class], 2);

        static::assertTrue($strategy->shouldRetry(new RuntimeException('boom'), 1));
        static::assertTrue($strategy->shouldRetry(new RuntimeException('boom'), 2));
        static::assertFalse($strategy->shouldRetry(new RuntimeException('boom'), 3));
    }

    public function test_retries_everything_outside_the_excluded_types(): void
    {
        $strategy = new AnyThrowableExcept([InvalidLogicException::class], 3);

        static::assertTrue($strategy->shouldRetry(new RuntimeException('boom'), 1));
        static::assertFalse($strategy->shouldRetry(InvalidLogicException::because('nope'), 1));
    }

    public function test_subclasses_of_an_excluded_type_are_also_excluded(): void
    {
        $strategy = new AnyThrowableExcept([RuntimeException::class], 3);

        static::assertFalse($strategy->shouldRetry(new OutOfBoundsException('boom'), 1));
    }

    public function test_throws_exception_for_zero_limit(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Retry limit must be greater than 0');

        new AnyThrowableExcept([InvalidLogicException::class], 0);
    }
}
