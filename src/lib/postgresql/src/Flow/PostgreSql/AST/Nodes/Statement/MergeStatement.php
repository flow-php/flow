<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\MergeStmt;

/**
 * @implements Statement<MergeStmt>
 */
final readonly class MergeStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private MergeStmt $stmt,
    ) {}

    public function raw(): MergeStmt
    {
        return $this->stmt;
    }
}
