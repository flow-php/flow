<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

use ArrayIterator;
use IteratorAggregate;

use function array_values;

/**
 * @implements IteratorAggregate<int, Node\Transaction|Node\Write>
 */
final readonly class Writes implements IteratorAggregate
{
    /**
     * @var list<Node\Transaction|Node\Write>
     */
    private array $writes;

    public function __construct(Node\Transaction|Node\Write ...$writes)
    {
        $this->writes = array_values($writes);
    }

    /**
     * @return list<Node\Transaction|Node\Write>
     */
    public function all(): array
    {
        return $this->writes;
    }

    /**
     * @return ArrayIterator<int, Node\Transaction|Node\Write>
     */
    public function getIterator(): ArrayIterator
    {
        return new ArrayIterator($this->writes);
    }

    public function merge(self $writes): self
    {
        return new self(...$this->writes, ...$writes->writes);
    }
}
