<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\IndexStmt;

/**
 * Represents CREATE INDEX statements.
 *
 * @implements Statement<IndexStmt>
 */
final readonly class IndexStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private IndexStmt $stmt,
    ) {}

    public function raw(): IndexStmt
    {
        return $this->stmt;
    }
}
