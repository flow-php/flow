<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Exception;

/**
 * Exception thrown when an invalid table reference is encountered.
 */
final class InvalidTableException extends QueryBuilderException
{
    public static function emptyArray(string $context) : self
    {
        return new self(\sprintf('%s cannot be empty', $context));
    }

    public static function invalidType(string $expected, mixed $actual) : self
    {
        return new self(\sprintf('Expected %s, got %s', $expected, \get_debug_type($actual)));
    }

    public static function invalidValue(string $context, mixed $value) : self
    {
        return new self(\sprintf('Invalid value for %s: %s', $context, \get_debug_type($value)));
    }
}
