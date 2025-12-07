<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\{TransactionStmt, TransactionStmtKind};

final readonly class SavepointBuilder implements SavepointFinalStep
{
    private function __construct(
        private string $name,
        private bool $isRelease,
    ) {
    }

    public static function create(string $name) : SavepointFinalStep
    {
        return new self($name, false);
    }

    public static function release(string $name) : SavepointFinalStep
    {
        return new self($name, true);
    }

    public function toAst() : TransactionStmt
    {
        $stmt = new TransactionStmt();

        if ($this->isRelease) {
            $stmt->setKind(TransactionStmtKind::TRANS_STMT_RELEASE);
        } else {
            $stmt->setKind(TransactionStmtKind::TRANS_STMT_SAVEPOINT);
        }

        $stmt->setSavepointName($this->name);

        return $stmt;
    }
}
