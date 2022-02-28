<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class CachedRow
{
    private string $cacheId;

    /**
     * @var Row
     */
    private Row $row;

    public function __construct(Row $row, string $cacheId)
    {
        $this->row = $row;
        $this->cacheId = $cacheId;
    }

    public static function fromRows(Rows $rows, string $cacheId) : self
    {
        if ($rows->count() !== 1) {
            throw new InvalidArgumentException("Cached row can't be created from multiple Rows.");
        }

        return new self($rows->first(), $cacheId);
    }

    public function cacheId() : string
    {
        return $this->cacheId;
    }

    public function row() : Row
    {
        return $this->row;
    }

    public function toRows() : Rows
    {
        return new Rows($this->row);
    }
}
