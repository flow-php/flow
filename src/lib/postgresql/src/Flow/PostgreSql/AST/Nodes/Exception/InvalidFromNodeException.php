<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Exception;

use Flow\PostgreSql\Protobuf\AST\Node;

final class InvalidFromNodeException extends \RuntimeException
{
    public static function invalidNode(Node $node): self
    {
        return new self(\sprintf('Invalid FROM clause node type: %s', $node->getNode() ?? 'unknown'));
    }
}
