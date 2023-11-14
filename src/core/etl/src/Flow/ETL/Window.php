<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\EntryReference;

final class Window
{
    /**
     * @var array<EntryReference>
     */
    private array $orderBy;

    /**
     * @var array<EntryReference>
     */
    private array $partitions;

    public function __construct()
    {
        $this->partitions = [];
        $this->orderBy = [];
    }

    /**
     * @return array<EntryReference>
     */
    public function order() : array
    {
        return $this->orderBy;
    }

    public function orderBy(EntryReference $ref, EntryReference ...$refs) : self
    {
        \array_unshift($refs, $ref);

        $this->orderBy = $refs;

        return $this;
    }

    public function partitionBy(EntryReference $ref, EntryReference ...$refs) : self
    {
        \array_unshift($refs, $ref);

        $this->partitions = $refs;
        $this->orderBy = $refs;

        return $this;
    }

    /**
     * @return array<EntryReference>
     */
    public function partitions() : array
    {
        return $this->partitions;
    }
}
