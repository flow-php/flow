<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use ArrayIterator;
use IteratorAggregate;

use function array_values;

/**
 * @implements IteratorAggregate<int, Node\Transaction|Node\Write>
 */
final readonly class Sinks implements IteratorAggregate
{
    /**
     * @var list<Node\Transaction|Node\Write>
     */
    private array $sinks;

    public function __construct(Node\Transaction|Node\Write ...$sinks)
    {
        $this->sinks = array_values($sinks);
    }

    /**
     * @return list<Node\Transaction|Node\Write>
     */
    public function all(): array
    {
        return $this->sinks;
    }

    /**
     * @return ArrayIterator<int, Node\Transaction|Node\Write>
     */
    public function getIterator(): ArrayIterator
    {
        return new ArrayIterator($this->sinks);
    }

    public function merge(self $sinks): self
    {
        return new self(...$this->sinks, ...$sinks->sinks);
    }
}
