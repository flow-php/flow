<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\HashJoin;

use function Flow\ETL\DSL\rows;
use Flow\ETL\Join\Expression;
use Flow\ETL\{Row, Rows};

final class Bucket implements \Countable
{
    private ?Rows $rows;

    /**
     * @var array<string, Row>
     */
    private array $rowsArray;

    /**
     * @var array<string, int>
     */
    private array $rowsMatches = [];

    /**
     * @param string $hash - hash of the bucket calculated from join expression columns and row
     */
    public function __construct(public readonly string $hash)
    {
        $this->rowsArray = [];
        $this->rows = null;
    }

    public function add(Row $row) : void
    {
        $this->rowsArray[$rowHash = $row->hash()] = $row;
        $this->rowsMatches[$rowHash] = 0;
        $this->rows = null;
    }

    public function count() : int
    {
        return \count($this->rowsArray);
    }

    public function findMatch(Row $row, Expression $expression) : ?Row
    {
        foreach ($this->rowsArray as $hash => $bucketRow) {
            if ($expression->meet($row, $bucketRow)) {
                $this->rowsMatches[$hash]++;

                return $bucketRow;
            }
        }

        return null;
    }

    public function rows() : Rows
    {
        if ($this->rows === null) {
            $this->rows = rows(...$this->rowsArray);
        }

        return $this->rows;
    }

    /**
     * @return array<Row>
     */
    public function unmatchedRows() : array
    {
        $unmatchedRows = [];

        foreach ($this->rowsArray as $hash => $bucketRow) {
            if ($this->rowsMatches[$hash] === 0) {
                $unmatchedRows[$hash] = $bucketRow;
            }
        }

        return $unmatchedRows;
    }
}
