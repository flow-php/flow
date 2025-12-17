<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\CommentStmt;

/**
 * @implements Statement<CommentStmt>
 */
final readonly class CommentStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private CommentStmt $stmt,
    ) {
    }

    public function raw() : CommentStmt
    {
        return $this->stmt;
    }
}
