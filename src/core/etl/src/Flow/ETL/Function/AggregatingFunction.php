<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

interface AggregatingFunction extends FunctionTree
{
    /**
     * @param list<int> $indices the rows of $rows that belong to this aggregate, ascending
     */
    public function aggregate(Rows $rows, array $indices, FlowContext $context): void;

    /**
     * Folds $other's partial state into this one; $other aggregated rows that come after this one's.
     *
     * @throws InvalidArgumentException $other is not the same class
     */
    public function merge(AggregatingFunction $other, FlowContext $context): void;

    /**
     * Decided in the constructor, never mutated.
     */
    public function outputName(): string;

    /**
     * @return null|list<Reference> references this aggregator reads, or null when they cannot be
     *                              statically enumerated (disables spill column pruning)
     */
    public function references(): ?array;

    /**
     * An OptionalType return declares the produced column nullable; there is no nullable() peer.
     *
     * @return Type<mixed>
     */
    public function returns(): Type;

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function value(): mixed;
}
