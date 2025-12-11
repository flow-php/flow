<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Exception;

/**
 * Exception thrown when a node type is not supported for conversion.
 */
final class UnsupportedNodeException extends QueryBuilderException
{
    public static function cannotReconstruct(string $className) : self
    {
        return new self(\sprintf('Cannot reconstruct %s from AST', $className));
    }

    public static function forNodeType(string $nodeType) : self
    {
        return new self(\sprintf('Unsupported node type: %s', $nodeType));
    }
}
