<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\Types\Type;

/**
 * Wrapper for values that need explicit type specification.
 *
 * Use when automatic type detection doesn't produce the desired PostgreSQL type.
 */
final readonly class TypedValue
{
    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        public mixed $value,
        public Type $type,
    ) {
    }
}
