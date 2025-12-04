<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Exception;

/**
 * Exception thrown when AST node structure is invalid for conversion.
 */
final class InvalidAstException extends QueryBuilderException
{
    public static function invalidFieldValue(string $field, string $nodeType, string $reason) : self
    {
        return new self(\sprintf('Invalid value for field "%s" in %s node: %s', $field, $nodeType, $reason));
    }

    public static function missingRequiredField(string $field, string $nodeType) : self
    {
        return new self(\sprintf('Missing required field "%s" in %s node', $field, $nodeType));
    }

    public static function unexpectedNodeType(string $expected, string $actual) : self
    {
        return new self(\sprintf('Expected %s node, got %s', $expected, $actual));
    }
}
