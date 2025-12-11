<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\{TransactionStmt, TransactionStmtKind};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class CommitBuilder implements CommitOptionsStep
{
    use AstToSql;

    private function __construct(
        private ?bool $chain = null,
    ) {
    }

    public static function create() : CommitOptionsStep
    {
        return new self();
    }

    public function andChain() : CommitFinalStep
    {
        return new self(true);
    }

    public function andNoChain() : CommitFinalStep
    {
        return new self(false);
    }

    public function toAst() : TransactionStmt
    {
        $stmt = new TransactionStmt();
        $stmt->setKind(TransactionStmtKind::TRANS_STMT_COMMIT);

        if ($this->chain !== null) {
            $stmt->setChain($this->chain);
        }

        return $stmt;
    }
}
