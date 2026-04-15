<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\MappingException;
use PHPUnit\Framework\TestCase;

final class MappingExceptionTest extends TestCase
{
    public function test_mapping_failed_carries_class_and_reason_in_message() : void
    {
        $exception = MappingException::mappingFailed('App\\UserDto', 'invalid shape');

        self::assertSame('Failed to map row to "App\\UserDto": invalid shape', $exception->getMessage());
        self::assertNull($exception->getPrevious());
    }

    public function test_mapping_failed_preserves_previous_throwable() : void
    {
        $previous = new \RuntimeException('root cause');

        $exception = MappingException::mappingFailed('App\\UserDto', 'invalid shape', $previous);

        self::assertSame($previous, $exception->getPrevious());
        self::assertSame('Failed to map row to "App\\UserDto": invalid shape', $exception->getMessage());
    }

    public function test_property_not_found_message() : void
    {
        $exception = MappingException::propertyNotFound('App\\UserDto', 'email');

        self::assertSame('Property "email" not found on class "App\\UserDto"', $exception->getMessage());
    }
}
