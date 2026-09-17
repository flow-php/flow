<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Transparency;

use function array_slice;

/**
 * The one root of a plan with several consumers of one prefix (Polars IR::SinkMultiple). children()[0] is the
 * Result - the spine - and every further child is a sink root that re-enters the spine's nodes by identity.
 */
final readonly class Outputs implements Node
{
    public function __construct(
        private Result $result,
        private Sinks $sinks,
    ) {
        if ($sinks->count() === 0) {
            throw InvalidLogicException::because('Outputs needs two or more consumers');
        }
    }

    /**
     * @return non-empty-list<Result|Transaction|Write>
     */
    public function children(): array
    {
        return [$this->result, ...$this->sinks->all()];
    }

    public function sinks(): Sinks
    {
        return $this->sinks;
    }

    public function withChildren(array $children): self
    {
        if ($children === $this->children()) {
            return $this;
        }

        $result = $children[0] instanceof Result
            ? $children[0]
            : throw InvalidLogicException::resultRewritten($children[0]::class);
        $sinks = [];

        foreach (array_slice($children, 1) as $child) {
            $sinks[] =
                $child instanceof Write || $child instanceof Transaction
                    ? $child
                    : throw InvalidLogicException::sinkRootRewritten($child::class);
        }

        return new self($result, new Sinks(...$sinks));
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
