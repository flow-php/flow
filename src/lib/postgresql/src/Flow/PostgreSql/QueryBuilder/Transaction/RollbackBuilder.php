<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\{TransactionStmt, TransactionStmtKind};
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class RollbackBuilder implements RollbackOptionsStep
{
    use AstToSql;

    private function __construct(
        private ?string $savepointName = null,
        private ?bool $chain = null,
    ) {
    }

    public static function create() : RollbackOptionsStep
    {
        return new self();
    }

    public function andChain() : RollbackFinalStep
    {
        return new self(
            $this->savepointName,
            true,
        );
    }

    public function andNoChain() : RollbackFinalStep
    {
        return new self(
            $this->savepointName,
            false,
        );
    }

    public function toAst() : TransactionStmt
    {
        $stmt = new TransactionStmt();

        if ($this->savepointName !== null) {
            $stmt->setKind(TransactionStmtKind::TRANS_STMT_ROLLBACK_TO);
            $stmt->setSavepointName($this->savepointName);
        } else {
            $stmt->setKind(TransactionStmtKind::TRANS_STMT_ROLLBACK);
        }

        if ($this->chain !== null) {
            $stmt->setChain($this->chain);
        }

        return $stmt;
    }

    public function toSavepoint(string $name) : RollbackFinalStep
    {
        return new self(
            $name,
            $this->chain,
        );
    }
}
