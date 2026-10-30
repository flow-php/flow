<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\Window\FrameBound;
use Flow\ETL\Functions\Window\PeerFrame;
use Flow\ETL\Functions\Window\RowsFrame;
use Flow\ETL\Functions\Window\WholePartitionFrame;
use Flow\ETL\Functions\Window\WindowFrame;
use Flow\ETL\Kernels\Sort\SortColumn;
use SensitiveParameter;

use function array_map;
use function array_unshift;
use function array_values;

final class Window implements Describable
{
    /**
     * @param array<Reference> $partitions
     * @param list<SortColumn> $orderBy
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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            array_values($rebuild->all($data['partitions'] ?? null, Reference::class)),
            array_values($rebuild->all($data['order_by'] ?? null, SortColumn::class)),
            ($data['frame'] ?? null) === null ? null : $rebuild->of($data['frame'], WindowFrame::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'partitions' => $this->partitions,
            'order_by' => $this->orderBy,
            'frame' => $this->frame,
        ]);
    }

    /**
     * @return list<SortColumn>
     */
    public function order(): array
    {
        return $this->orderBy;
    }

    /**
     * A reference orders ascending.
     */
    public function orderBy(Reference|SortColumn|string $column, Reference|SortColumn|string ...$columns): self
    {
        return new self(
            $this->partitions,
            array_map(
                static fn(Reference|SortColumn|string $column): SortColumn => $column instanceof SortColumn
                    ? $column
                    : UnresolvedReference::init($column)->asc(),
                [$column, ...array_values($columns)],
            ),
            $this->frame,
        );
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
