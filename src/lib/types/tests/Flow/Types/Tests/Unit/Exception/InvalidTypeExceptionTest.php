<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Exception;

use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class InvalidTypeExceptionTest extends TestCase
{
    public function test_no_common_type_names_every_type_in_order(): void
    {
        static::assertSame(
            'Cannot combine types "datetime", "integer", "string" - an explicit cast is required.',
            InvalidTypeException::noCommonType(type_datetime(), type_integer(), type_string())->getMessage(),
        );
    }
}
