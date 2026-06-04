<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Exception;

use function get_debug_type;
use function sprintf;

/**
 * Exception thrown when an invalid expression is encountered.
 */
final class InvalidExpressionException extends QueryBuilderException
{
    public static function emptyArray(string $context): self
    {
        return new self(sprintf('%s cannot be empty', $context));
    }

    public static function invalidType(string $expected, mixed $actual): self
    {
        return new self(sprintf('Expected %s, got %s', $expected, get_debug_type($actual)));
    }

    public static function keywordConstruct(string $name, string $helper): self
    {
        return new self(sprintf(
            'func() cannot build "%s" because PostgreSQL parses it as a built-in keyword construct, not a catalog function. Use the %s() DSL helper instead.',
            $name,
            $helper,
        ));
    }

    public static function invalidValue(string $context, mixed $value): self
    {
        return new self(sprintf('Invalid value for %s: %s', $context, get_debug_type($value)));
    }
}
