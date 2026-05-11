<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;

/**
 * @implements Statement<DeleteStmt>
 */
final readonly class DeleteStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private DeleteStmt $stmt,
    ) {}

    public function raw(): DeleteStmt
    {
        return $this->stmt;
    }
}
