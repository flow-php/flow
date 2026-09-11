<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

use function count;

/**
 * @implements \IteratorAggregate<int, Table>
 */
final readonly class Tables implements Countable, IteratorAggregate
{
    /**
     * @param array<int, Table> $tables
     */
    public function __construct(
        private array $tables,
    ) {}

    /**
     * @return array<int, Table>
     */
    public function all(): array
    {
        return $this->tables;
    }

    public function count(): int
    {
        return count($this->tables);
    }

    public function first(): ?Table
    {
        return $this->tables[0] ?? null;
    }

    public function get(int $index): ?Table
    {
        return $this->tables[$index] ?? null;
    }

    /**
     * @return \Traversable<int, Table>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->tables);
    }

    /**
     * @assert-if-false Table $this->first()
     * @assert-if-false Table $this->last()
     */
    public function isEmpty(): bool
    {
        return count($this->tables) === 0;
    }

    /**
     * @assert-if-true Table $this->first()
     * @assert-if-true Table $this->last()
     */
    public function isSingle(): bool
    {
        return count($this->tables) === 1;
    }

    public function last(): ?Table
    {
        if (count($this->tables) === 0) {
            return null;
        }

        return $this->tables[count($this->tables) - 1];
    }
}
