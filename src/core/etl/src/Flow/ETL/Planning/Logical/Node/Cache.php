<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Storage\RowsStorage;

/**
 * A write-through: every batch is yielded onward as it is cached, so there is no materialisation barrier, but the cache is an effect outside the stream.
 */
final readonly class Cache implements Node
{
    /**
     * @param null|int<1, max> $batchSize
     */
    public function __construct(
        private Node $input,
        public ?string $id,
        public ?int $batchSize,
        public ?RowsStorage $cache,
    ) {}

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [$this->input];
    }

    public function withChildren(array $children): self
    {
        return $children[0] === $this->input
            ? $this
            : new self($children[0], $this->id, $this->batchSize, $this->cache);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
