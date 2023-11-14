<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Window\Average;
use Flow\ETL\Window\Count;
use Flow\ETL\Window\DensRank;
use Flow\ETL\Window\Rank;
use Flow\ETL\Window\RowNumber;
use Flow\ETL\Window\Sum;
use Flow\ETL\Window\WindowFunction;

final class _Window
{
    private ?WindowFunction $function = null;

    /**
     * @var array<EntryReference>
     */
    private array $orderBy = [];

    /**
     * @var array<EntryReference>
     */
    private array $partitions;

    private function __construct(EntryReference ...$partitions)
    {
        $this->partitions = $partitions;
    }

    public static function partitionBy(EntryReference ...$partitions) : self
    {
        return new self(...$partitions);
    }

    public function avg(EntryReference $ref) : self
    {
        $this->function = new Average($ref);

        return $this;
    }

    public function count(EntryReference $ref) : self
    {
        $this->function = new Count($ref);

        return $this;
    }

    public function densRank() : self
    {
        $this->function = new DensRank();

        return $this;
    }

    /**
     * @throws RuntimeException
     */
    public function function() : WindowFunction
    {
        if ($this->function === null) {
            throw new RuntimeException('Window function is not set');
        }

        return $this->function;
    }

    /**
     * @return EntryReference[]
     */
    public function order() : array
    {
        return $this->orderBy;
    }

    public function orderBy(EntryReference ...$refs) : self
    {
        $this->orderBy = $refs;

        return $this;
    }

    /**
     * @return EntryReference[]
     */
    public function partitions() : array
    {
        return $this->partitions;
    }

    public function rank() : self
    {
        $this->function = new Rank();

        return $this;
    }

    public function rowNumber() : self
    {
        $this->function = new RowNumber();

        return $this;
    }

    public function sum(EntryReference $ref) : self
    {
        $this->function = new Sum($ref);

        return $this;
    }
}
