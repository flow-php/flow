<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\PrepareStmt;

/**
 * @implements Statement<PrepareStmt>
 */
final readonly class PrepareStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private PrepareStmt $stmt,
    ) {}

    public function raw(): PrepareStmt
    {
        return $this->stmt;
    }
}
