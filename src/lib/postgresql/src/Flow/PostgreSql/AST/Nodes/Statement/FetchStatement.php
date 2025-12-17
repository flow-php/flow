<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\FetchStmt;

/**
 * @implements Statement<FetchStmt>
 */
final readonly class FetchStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private FetchStmt $stmt,
    ) {
    }

    public function raw() : FetchStmt
    {
        return $this->stmt;
    }
}
