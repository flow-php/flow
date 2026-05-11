<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\Types\ValueType;

/**
 * Wrapper for values that need explicit type specification.
 *
 * Use when automatic type detection doesn't produce the desired PostgreSQL type.
 * The targetType specifies which PostgreSQL type the value should be converted to.
 */
final readonly class TypedValue
{
    public function __construct(
        public mixed $value,
        public ValueType $targetType,
    ) {}
}
