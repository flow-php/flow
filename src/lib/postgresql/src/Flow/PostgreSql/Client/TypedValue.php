<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\Types\ValueType;

/**
 * Use when automatic type detection picks the wrong PostgreSQL type.
 */
final readonly class TypedValue
{
    public function __construct(
        public mixed $value,
        public ValueType $targetType,
    ) {}
}
