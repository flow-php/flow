<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Bridge;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;

/**
 * Interface for all components that can be converted to/from AST nodes.
 */
interface AstConvertible
{
    /**
     * Reconstruct builder element from protobuf AST Node.
     *
     * @throws InvalidAstException When node structure is invalid
     * @throws UnsupportedNodeException When node type is not supported
     */
    public static function fromAst(Node $node): static;

    /**
     * Convert this builder element to a protobuf AST Node.
     */
    public function toAst(): Node;
}
