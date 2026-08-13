<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Exception;

use Flow\Types\Exception\MissingElementCastingException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\Types\DSL\type_string;

final class MissingElementCastingExceptionTest extends TestCase
{
    public function test_element_property(): void
    {
        $exception = new MissingElementCastingException(null, type_string(), 'name');

        static::assertSame('name', $exception->element);
    }

    public function test_message_names_the_element(): void
    {
        $exception = new MissingElementCastingException(null, type_string(), 'name');

        static::assertStringContainsString('"name"', $exception->getMessage());
        static::assertStringContainsString(type_string()->toString(), $exception->getMessage());
    }

    public function test_previous_is_chained(): void
    {
        $previous = new RuntimeException('root cause');

        static::assertSame(
            $previous,
            (new MissingElementCastingException(null, type_string(), 'name', $previous))->getPrevious(),
        );
    }

    public function test_value_and_type_properties(): void
    {
        $type = type_string();
        $exception = new MissingElementCastingException(null, $type, 'name');

        static::assertNull($exception->value);
        static::assertSame($type, $exception->type);
    }
}
