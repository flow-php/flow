<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\RuleStmt;

/**
 * Represents CREATE RULE statements.
 *
 * @implements Statement<RuleStmt>
 */
final readonly class RuleStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private RuleStmt $stmt,
    ) {
    }

    public function raw() : RuleStmt
    {
        return $this->stmt;
    }
}
