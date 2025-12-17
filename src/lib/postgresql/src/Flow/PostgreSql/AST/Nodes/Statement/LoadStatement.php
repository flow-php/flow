<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\LoadStmt;

/**
 * @implements Statement<LoadStmt>
 */
final readonly class LoadStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private LoadStmt $stmt,
    ) {
    }

    public function raw() : LoadStmt
    {
        return $this->stmt;
    }
}
