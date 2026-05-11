<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\SecLabelStmt;

/**
 * Represents SECURITY LABEL statements.
 *
 * @implements Statement<SecLabelStmt>
 */
final readonly class SecurityLabelStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private SecLabelStmt $stmt,
    ) {}

    public function raw(): SecLabelStmt
    {
        return $this->stmt;
    }
}
