<?php declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;

trait Limitable
{
    private ?int $limit = null;

    private int $yieldedRows = 0;

    public function changeLimit(int $limit) : void
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        $this->limit = $limit;
    }

    public function countRow() : void
    {
        $this->yieldedRows++;
    }

    public function limit() : ?int
    {
        return $this->limit;
    }

    public function reachedLimit() : bool
    {
        if ($this->limit === null) {
            return false;
        }

        return $this->yieldedRows >= $this->limit;
    }

    public function resetLimit() : void
    {
        $this->limit = null;
        $this->yieldedRows = 0;
    }
}
