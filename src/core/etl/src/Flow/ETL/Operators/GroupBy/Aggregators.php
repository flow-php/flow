<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use ArrayIterator;
use Countable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
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

    /**
     * @param list<int> $indices
     */
    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        foreach ($this->aggregators as $aggregator) {
            $aggregator->aggregate($rows, $indices, $backend);
        }
    }

    /**
     * The same aggregators without state: withChildren(children()) builds each one anew.
     */
    public function fresh(): self
    {
        return new self(...array_map(
            static fn(AggregatingFunction $aggregator): AggregatingFunction => $aggregator->withChildren(
                $aggregator->children(),
            ),
            $this->aggregators,
        ));
    }

    public function count(): int
    {
        return count($this->aggregators);
    }

    /**
     * Folds $other's partial states into these, pairwise; $other aggregated rows that come after these.
     */
    public function merge(self $other, Backend $backend): void
    {
        foreach ($this->aggregators as $position => $aggregator) {
            $aggregator->merge($other->aggregators[$position]->partial(), $backend);
        }
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
            $bound = $resolver->bind($aggregator, $schema, 'aggregate');
            $resolved[] = $bound;
        }

        return new self(...$resolved);
    }

    /**
     * @return null|list<Reference> union of references read by all aggregators, or null when any
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
