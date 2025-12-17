<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\Node;

/**
 * Fallback wrapper for any statement type not explicitly handled.
 *
 * @implements Statement<Node>
 */
final readonly class UnknownStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private Node $node,
    ) {
    }

    public function raw() : Node
    {
        return $this->node;
    }
}
