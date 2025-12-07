<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{LockStmt, Node, RangeVar};

final readonly class LockBuilder implements LockFinalStep
{
    /**
     * @param array<string> $tables
     */
    private function __construct(
        private array $tables,
        private LockMode $mode = LockMode::ACCESS_EXCLUSIVE,
        private bool $nowait = false,
    ) {
    }

    public static function create(string ...$tables) : LockFinalStep
    {
        return new self($tables);
    }

    public function accessExclusive() : LockFinalStep
    {
        return new self($this->tables, LockMode::ACCESS_EXCLUSIVE, $this->nowait);
    }

    public function accessShare() : LockFinalStep
    {
        return new self($this->tables, LockMode::ACCESS_SHARE, $this->nowait);
    }

    public function exclusive() : LockFinalStep
    {
        return new self($this->tables, LockMode::EXCLUSIVE, $this->nowait);
    }

    public function inMode(LockMode $mode) : LockFinalStep
    {
        return new self($this->tables, $mode, $this->nowait);
    }

    public function nowait() : LockFinalStep
    {
        return new self($this->tables, $this->mode, true);
    }

    public function rowExclusive() : LockFinalStep
    {
        return new self($this->tables, LockMode::ROW_EXCLUSIVE, $this->nowait);
    }

    public function rowShare() : LockFinalStep
    {
        return new self($this->tables, LockMode::ROW_SHARE, $this->nowait);
    }

    public function share() : LockFinalStep
    {
        return new self($this->tables, LockMode::SHARE, $this->nowait);
    }

    public function shareRowExclusive() : LockFinalStep
    {
        return new self($this->tables, LockMode::SHARE_ROW_EXCLUSIVE, $this->nowait);
    }

    public function shareUpdateExclusive() : LockFinalStep
    {
        return new self($this->tables, LockMode::SHARE_UPDATE_EXCLUSIVE, $this->nowait);
    }

    public function toAst() : LockStmt
    {
        $stmt = new LockStmt();
        $stmt->setMode($this->mode->value);
        $stmt->setNowait($this->nowait);

        $relations = [];

        foreach ($this->tables as $table) {
            $rangeVar = new RangeVar();

            $parts = \explode('.', $table);

            if (\count($parts) === 2) {
                $rangeVar->setSchemaname($parts[0]);
                $rangeVar->setRelname($parts[1]);
            } else {
                $rangeVar->setRelname($table);
            }

            $rangeVar->setInh(true);
            $rangeVar->setRelpersistence('p');

            $node = new Node();
            $node->setRangeVar($rangeVar);
            $relations[] = $node;
        }

        $stmt->setRelations($relations);

        return $stmt;
    }
}
