<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use ArrayIterator;
use Countable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row;
use IteratorAggregate;
use Traversable;

use function array_map;
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
}
