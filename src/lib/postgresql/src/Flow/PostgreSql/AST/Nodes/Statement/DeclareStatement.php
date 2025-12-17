<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\DeclareCursorStmt;

/**
 * @implements Statement<DeclareCursorStmt>
 */
final readonly class DeclareStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private DeclareCursorStmt $stmt,
    ) {
    }

    public function raw() : DeclareCursorStmt
    {
        return $this->stmt;
    }
}
