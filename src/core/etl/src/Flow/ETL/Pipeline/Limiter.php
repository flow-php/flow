<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;

final class Limiter
{
    private Rows $latest;

    private int $total;

    public function __construct(private readonly int|null $limit)
    {
        if ($this->limit !== null && $this->limit <= 0) {
            throw new InvalidArgumentException('Reductor limit can be only greater than zero');
        }

        $this->total = 0;
        $this->latest = new Rows();
    }

    public function latest() : Rows
    {
        return $this->latest;
    }

    public function limit(Rows $rows) : ?Rows
    {
        if ($this->limit === null) {
            return $rows;
        }

        if ($this->limitReached()) {
            return null;
        }

        $this->total += $rows->count();
        $this->latest = $rows;

        if ($this->total > $this->limit) {
            $diff = $this->total - $this->limit;

            $this->total = $this->limit;
            $this->latest = $rows->dropRight($diff);

            return $this->latest;
        }

        return $rows;
    }

    public function limitReached() : bool
    {
        return $this->total >= $this->limit;
    }

    public function limitTransformed(Rows $rows) : Rows
    {
        if ($this->limit === null) {
            return $rows;
        }

        $extra = $rows->count() - $this->latest->count();
        $this->total += $extra;
        $this->latest = $rows;

        if ($this->total > $this->limit) {
            $diff = $this->total - $this->limit;

            $this->total = $this->limit;
            $this->latest = $rows->dropRight($diff);

            return $this->latest;
        }

        return $rows;
    }
}
