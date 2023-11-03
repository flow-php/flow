<?php

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;

trait Limitable
{
    private int $yieldedRows = 0;

    private ?int $limit = null;

    public function limit(int $limit): void
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        $this->limit = $limit;
    }

    public function isLimited() : bool
    {
        return $this->limit !== null;
    }

    public function getLimit() : int
    {
        if ($this->limit === null) {
            throw new RuntimeException('Extractor is not limited');
        }

        return $this->limit;
    }

    public function countRows(Rows $rows) : void
    {
        $this->yieldedRows += $rows->count();
    }

    public function reachedLimit() : bool
    {
        if ($this->limit === null) {
            return false;
        }

        return $this->yieldedRows >= $this->limit;
    }

    public function yieldedRows() : int
    {
        return $this->yieldedRows;
    }

    public function rowsAboveTheLimit() : int
    {
        if ($this->limit === null) {
            throw new RuntimeException('Extractor is not limited');
        }

        if (!$this->reachedLimit()) {
            throw new RuntimeException('Extractor did not reach the limit');
        }

        return $this->yieldedRows - $this->limit;
    }
}