<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\ClusterStmt;

/**
 * @implements Statement<ClusterStmt>
 */
final readonly class ClusterStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ClusterStmt $stmt,
    ) {
    }

    public function raw() : ClusterStmt
    {
        return $this->stmt;
    }
}
