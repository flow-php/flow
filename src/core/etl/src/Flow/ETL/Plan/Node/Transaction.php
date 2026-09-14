<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Transaction as FlowTransaction;

use function array_values;

/**
 * A ROOT grouping sink roots that commit together; never on a spine.
 */
final readonly class Transaction implements Node
{
    /**
     * @var list<Write>
     */
    private array $sinks;

    public function __construct(
        public FlowTransaction $transaction,
        Write ...$sinks,
    ) {
        if ($sinks === []) {
            throw new InvalidArgumentException('At least one loader must be provided');
        }

        $this->sinks = array_values($sinks);
    }

    /**
     * @return list<Write> the sibling ROOTS this transaction commits together - never a row input
     */
    public function children(): array
    {
        return $this->sinks;
    }

    /**
     * @param list<Node> $children
     */
    public function withChildren(array $children): self
    {
        $writes = [];

        foreach ($children as $child) {
            $writes[] = $child instanceof Write
                ? $child
                : throw InvalidLogicException::sinkRootRewritten($child::class);
        }

        return $writes === $this->sinks ? $this : new self($this->transaction, ...$writes);
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
