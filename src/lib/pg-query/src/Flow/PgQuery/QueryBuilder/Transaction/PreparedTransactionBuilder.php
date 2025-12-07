<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\{TransactionStmt, TransactionStmtKind};

final readonly class PreparedTransactionBuilder implements PreparedTransactionFinalStep
{
    private function __construct(
        private string $gid,
        private int $kind,
    ) {
    }

    public static function commitPrepared(string $gid) : PreparedTransactionFinalStep
    {
        return new self($gid, TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED);
    }

    public static function prepare(string $gid) : PreparedTransactionFinalStep
    {
        return new self($gid, TransactionStmtKind::TRANS_STMT_PREPARE);
    }

    public static function rollbackPrepared(string $gid) : PreparedTransactionFinalStep
    {
        return new self($gid, TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED);
    }

    public function toAst() : TransactionStmt
    {
        $stmt = new TransactionStmt();
        $stmt->setKind($this->kind);
        $stmt->setGid($this->gid);

        return $stmt;
    }
}
