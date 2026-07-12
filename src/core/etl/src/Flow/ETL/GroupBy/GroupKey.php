<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use ArrayIterator;
use IteratorAggregate;
use Stringable;
use Traversable;

use function serialize;

/**
 * @implements IteratorAggregate<string, mixed>
 */
final readonly class GroupKey implements IteratorAggregate, Stringable
{
    /**
     * @param array<string, mixed> $values ref name => key value
     */
    public function __construct(
        private array $values,
    ) {}

    public function __toString(): string
    {
        return serialize($this->values);
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->values);
    }
}
