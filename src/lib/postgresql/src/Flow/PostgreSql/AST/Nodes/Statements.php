<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

use function count;

/**
 * @implements \IteratorAggregate<int, Statement<mixed>>
 */
final readonly class Statements implements Countable, IteratorAggregate
{
    /**
     * @param array<int, Statement<mixed>> $statements
     */
    public function __construct(
        private array $statements,
    ) {}

    /**
     * @return array<int, Statement<mixed>>
     */
    public function all(): array
    {
        return $this->statements;
    }

    public function count(): int
    {
        return count($this->statements);
    }

    /**
     * @return null|Statement<mixed>
     */
    public function first(): ?Statement
    {
        return $this->statements[0] ?? null;
    }

    /**
     * @return null|Statement<mixed>
     */
    public function get(int $index): ?Statement
    {
        return $this->statements[$index] ?? null;
    }

    /**
     * @return \Traversable<int, Statement<mixed>>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->statements);
    }

    /**
     * @assert-if-false Statement<mixed> $this->first()
     * @assert-if-false Statement<mixed> $this->last()
     */
    public function isEmpty(): bool
    {
        return count($this->statements) === 0;
    }

    /**
     * @assert-if-true Statement<mixed> $this->first()
     * @assert-if-true Statement<mixed> $this->last()
     */
    public function isSingle(): bool
    {
        return count($this->statements) === 1;
    }

    /**
     * @return null|Statement<mixed>
     */
    public function last(): ?Statement
    {
        if (count($this->statements) === 0) {
            return null;
        }

        return $this->statements[count($this->statements) - 1];
    }
}
