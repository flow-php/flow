<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\ReindexStmt;

/**
 * @implements Statement<ReindexStmt>
 */
final readonly class ReindexStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ReindexStmt $stmt,
    ) {
    }

    public function raw() : ReindexStmt
    {
        return $this->stmt;
    }
}
