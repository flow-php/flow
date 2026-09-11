<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Window\FrameBound;
use Flow\ETL\Window\PeerFrame;
use Flow\ETL\Window\RowsFrame;
use Flow\ETL\Window\WholePartitionFrame;
use Flow\ETL\Window\WindowFrame;

use function array_unshift;

final class Window
{
    /**
     * @param array<Reference> $partitions
     * @param array<Reference> $orderBy
     */
    public function __construct(
        private readonly array $partitions = [],
        private readonly array $orderBy = [],
        private readonly ?WindowFrame $frame = null,
    ) {}

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

        return new self($this->partitions, $refs, $this->frame);
    }

    public function partitionBy(Reference $ref, Reference ...$refs): self
    {
        array_unshift($refs, $ref);

        return new self($refs, $this->orderBy, $this->frame);
    }

    public function partitions(): References
    {
        return References::init(...$this->partitions);
    }

    public function rowsBetween(FrameBound $start, FrameBound $end): self
    {
        return new self($this->partitions, $this->orderBy, new RowsFrame($start, $end));
    }
}
