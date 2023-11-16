<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Reference;

final class Window
{
    /**
     * @var array<Reference>
     */
    private array $orderBy;

    /**
     * @var array<Reference>
     */
    private array $partitions;

    public function __construct()
    {
        $this->partitions = [];
        $this->orderBy = [];
    }

    /**
     * @return array<Reference>
     */
    public function order() : array
    {
        return $this->orderBy;
    }

    public function orderBy(Reference $ref, Reference ...$refs) : self
    {
        \array_unshift($refs, $ref);

        $this->orderBy = $refs;

        return $this;
    }

    public function partitionBy(Reference $ref, Reference ...$refs) : self
    {
        \array_unshift($refs, $ref);

        $this->partitions = $refs;
        $this->orderBy = $refs;

        return $this;
    }

    /**
     * @return array<Reference>
     */
    public function partitions() : array
    {
        return $this->partitions;
    }
}
