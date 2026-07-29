<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Reference;
use Flow\ETL\Window\FrameBound;
use Flow\ETL\Window\PeerFrame;
use Flow\ETL\Window\RowsFrame;
use Flow\ETL\Window\WholePartitionFrame;
use Flow\ETL\Window\WindowFrame;

use function array_unshift;

final class Window
{
    private ?WindowFrame $frame;

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
        $this->frame = null;
    }

    public function frame(): WindowFrame
    {
        if ($this->frame !== null) {
            return $this->frame;
        }

        return [] === $this->orderBy ? new WholePartitionFrame() : new PeerFrame($this->orderBy);
    }

    /**
     * @return array<Reference>
     */
    public function order(): array
    {
        return $this->orderBy;
    }

    public function orderBy(Reference $ref, Reference ...$refs): self
    {
        array_unshift($refs, $ref);

        $this->orderBy = $refs;

        return $this;
    }

    public function partitionBy(Reference $ref, Reference ...$refs): self
    {
        array_unshift($refs, $ref);

        $this->partitions = $refs;

        return $this;
    }

    /**
     * @return array<Reference>
     */
    public function partitions(): array
    {
        return $this->partitions;
    }

    public function rowsBetween(FrameBound $start, FrameBound $end): self
    {
        $this->frame = new RowsFrame($start, $end);

        return $this;
    }
}
