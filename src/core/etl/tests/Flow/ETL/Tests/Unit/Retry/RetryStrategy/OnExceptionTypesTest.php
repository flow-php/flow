<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\RetryStrategy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Retry\RetryStrategy\OnExceptionTypes;
use PHPUnit\Framework\TestCase;

final class OnExceptionTypesTest extends TestCase
{
    public function test_custom_exception_types() : void
    {
        $customException = new class('test') extends \Exception {};
        $anotherException = new class('test') extends \RuntimeException {};

        $strategy = new OnExceptionTypes([\Exception::class], 3);

        self::assertTrue($strategy->shouldRetry($customException, 1));
        self::assertTrue($strategy->shouldRetry($anotherException, 1));
    }

    public function test_empty_array_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Exception types cannot be empty. Use AnyThrowable strategy to retry on any throwable.');

        new OnExceptionTypes([], 3);
    }

    public function test_error_types_are_supported() : void
    {
        $strategy = new OnExceptionTypes([\Error::class], 3);

        self::assertTrue($strategy->shouldRetry(new \Error('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \TypeError('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \Exception('test'), 1));
    }

    public function test_inheritance_with_specific_subclass() : void
    {
        $strategy = new OnExceptionTypes([\LogicException::class], 3);

        // Should match LogicException and its subclasses
        self::assertTrue($strategy->shouldRetry(new \LogicException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \InvalidArgumentException('test'), 1));

        // Should not match Exception or RuntimeException
        self::assertFalse($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \RuntimeException('test'), 1));
    }

    public function test_invalid_class_name_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Class 'NonExistentClass' does not exist");

        /** @phpstan-ignore-next-line */
        new OnExceptionTypes(['NonExistentClass'], 3);
    }

    public function test_non_throwable_class_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Class 'stdClass' is not a Throwable");

        /** @phpstan-ignore-next-line */
        new OnExceptionTypes([\stdClass::class], 3);
    }

    public function test_respects_max_attempts() : void
    {
        $strategy = new OnExceptionTypes([\RuntimeException::class], 3);
        $exception = new \RuntimeException('test');

        self::assertTrue($strategy->shouldRetry($exception, 1));
        self::assertTrue($strategy->shouldRetry($exception, 2));
        self::assertTrue($strategy->shouldRetry($exception, 3));
        self::assertFalse($strategy->shouldRetry($exception, 4));
        self::assertFalse($strategy->shouldRetry($exception, 100));
    }

    public function test_retries_on_multiple_exception_types() : void
    {
        $strategy = new OnExceptionTypes([\RuntimeException::class, \UnexpectedValueException::class], 3);

        self::assertTrue($strategy->shouldRetry(new \RuntimeException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \UnexpectedValueException('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \LogicException('test'), 1));
    }

    public function test_retries_on_specific_exception_types() : void
    {
        $strategy = new OnExceptionTypes([\RuntimeException::class], 3);

        self::assertTrue($strategy->shouldRetry(new \RuntimeException('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertFalse($strategy->shouldRetry(new \LogicException('test'), 1));
    }

    public function test_supports_exception_inheritance() : void
    {
        $strategy = new OnExceptionTypes([\Exception::class], 3);

        // Should match Exception and all its subclasses
        self::assertTrue($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \RuntimeException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \LogicException('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \InvalidArgumentException('test'), 1));
    }

    public function test_throwable_interface_is_accepted() : void
    {
        $strategy = new OnExceptionTypes([\Throwable::class], 3);

        self::assertTrue($strategy->shouldRetry(new \Exception('test'), 1));
        self::assertTrue($strategy->shouldRetry(new \Error('test'), 1));
    }

    public function test_throws_exception_for_negative_limit() : void
    {
        $this->expectException(\Flow\ETL\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Retry limit must be greater than 0');

        new OnExceptionTypes([\RuntimeException::class], -1);
    }

    public function test_throws_exception_for_zero_limit() : void
    {
        $this->expectException(\Flow\ETL\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Retry limit must be greater than 0');

        new OnExceptionTypes([\RuntimeException::class], 0);
    }
}
