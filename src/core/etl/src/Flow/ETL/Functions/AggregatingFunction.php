<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

interface AggregatingFunction extends FunctionTree, Describable
{
    /**
     * @param list<int> $indices the rows of $rows that belong to this aggregate, ascending
     */
    public function aggregate(Rows $rows, array $indices, Backend $backend): void;

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
     * Folds a partial() of another instance into this one; that instance aggregated rows that come after this
     * one's (merge()'s contract).
     *
     * @param array<string, mixed> $partial
     */
    public function merge(array $partial, Backend $backend): void;

    /**
     * This aggregator's state as plain values of partialType(); an aggregator that saw no row returns its initial
     * state.
     *
     * @return array<string, mixed>
     */
    public function partial(): array;

    /**
     * The structure partial() returns; valid once the aggregator is resolved (Aggregators::resolved()).
     *
     * @return StructureType<array<string, mixed>>
     */
    public function partialType(): StructureType;

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function value(): mixed;
}
