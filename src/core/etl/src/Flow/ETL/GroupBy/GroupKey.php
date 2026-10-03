<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use ArrayIterator;
use IteratorAggregate;
use Stringable;
use Traversable;

/**
 * @implements IteratorAggregate<string, mixed>
 */
final readonly class GroupKey implements IteratorAggregate, Stringable
{
    /**
     * @param array<string, mixed> $values ref name => key value
     * @param string $identity what tells two keys apart: the serialized equality forms of the key, in ref order
     */
    public function __construct(
        private array $values,
        private string $identity,
    ) {}

    public function __toString(): string
    {
        return $this->identity;
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->values);
    }
}
