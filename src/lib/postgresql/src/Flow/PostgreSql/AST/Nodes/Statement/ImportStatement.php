<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\ImportForeignSchemaStmt;

/**
 * Represents IMPORT FOREIGN SCHEMA statements.
 *
 * @implements Statement<ImportForeignSchemaStmt>
 */
final readonly class ImportStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ImportForeignSchemaStmt $stmt,
    ) {
    }

    public function raw() : ImportForeignSchemaStmt
    {
        return $this->stmt;
    }
}
