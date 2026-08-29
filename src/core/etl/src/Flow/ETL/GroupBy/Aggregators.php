<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use ArrayIterator;
use Countable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Row;
use Flow\ETL\Schema;
use IteratorAggregate;
use Traversable;

use function array_map;
use function array_values;
use function count;

/**
 * @implements IteratorAggregate<array-key, AggregatingFunction>
 */
final readonly class Aggregators implements Countable, IteratorAggregate
{
    /**
     * @var array<array-key, AggregatingFunction>
     */
    private array $aggregators;

    public function __construct(AggregatingFunction ...$aggregators)
    {
        $this->aggregators = $aggregators;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        foreach ($this->aggregators as $aggregator) {
            $aggregator->aggregate($row, $context);
        }
    }

    public function cloned(): self
    {
        return new self(...array_map(
            static fn(AggregatingFunction $aggregator): AggregatingFunction => clone $aggregator,
            $this->aggregators,
        ));
    }

    public function count(): int
    {
        return count($this->aggregators);
    }

    public function first(): AggregatingFunction
    {
        if ($this->aggregators === []) {
            throw new InvalidArgumentException('Aggregators are empty.');
        }

        return $this->aggregators[0];
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->aggregators);
    }

    /**
     * Rebuilds every aggregator fresh - callers must bind before the first aggregate() call, or
     * accumulated state is discarded.
     */
    public function resolved(Schema $schema): self
    {
        $resolver = new ReferenceResolver();
        $resolved = [];

        foreach ($this->aggregators as $aggregator) {
            /** @var AggregatingFunction $bound an aggregate root is never a reference leaf */
            $bound = $resolver->resolve($aggregator, $schema);
            $resolver->assertResolved($bound, $schema);
            $resolved[] = $bound;
        }

        return new self(...$resolved);
    }

    /**
     * @return null|list<Row\Reference> union of references read by all aggregators, or null when any
     *                                  aggregator cannot enumerate them (disables spill column pruning)
     */
    public function references(): ?array
    {
        $references = [];

        foreach ($this->aggregators as $aggregator) {
            $aggregatorReferences = $aggregator->references();

            if ($aggregatorReferences === null) {
                return null;
            }

            foreach ($aggregatorReferences as $reference) {
                $references[$reference->base()] ??= $reference;
            }
        }

        return array_values($references);
    }
}
