<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

use function array_slice;
use function array_values;
use function count;

/**
 * The one root of a plan with several consumers of one prefix (Polars IR::SinkMultiple). children()[0] is the
 * Result - the spine - and every further child is a sink root that re-enters the spine's nodes by identity.
 */
final readonly class SinkMultiple implements Node
{
    /**
     * @var non-empty-list<Result|Transaction|Write>
     */
    private array $consumers;

    public function __construct(Result $result, Transaction|Write ...$sinks)
    {
        if (count($sinks) === 0) {
            throw InvalidLogicException::because('SinkMultiple needs two or more consumers');
        }

        $this->consumers = [$result, ...array_values($sinks)];
    }

    /**
     * @return non-empty-list<Result|Transaction|Write>
     */
    public function children(): array
    {
        return $this->consumers;
    }

    /**
     * @param list<Node> $children
     */
    public function withChildren(array $children): self
    {
        if ($children === $this->consumers) {
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

        return new self($result, ...$sinks);
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
