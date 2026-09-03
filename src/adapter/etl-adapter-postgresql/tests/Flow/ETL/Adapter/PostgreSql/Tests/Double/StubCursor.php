<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Double;

use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\RowMapper;
use Generator;
use RuntimeException;
use Traversable;

use function array_shift;
use function count;

final class StubCursor implements Cursor
{
    /**
     * @param list<array<string, mixed>> $rows
     */
    public function __construct(
        private array $rows = [],
    ) {}

    public function count(): int
    {
        return count($this->rows);
    }

    public function free(): void {}

    public function getIterator(): Traversable
    {
        return $this->iterate();
    }

    /**
     * @return Generator<int, array<string, mixed>>
     */
    public function iterate(): Generator
    {
        while (($row = $this->next()) !== null) {
            yield $row;
        }
    }

    public function map(RowMapper $mapper): Generator
    {
        throw new RuntimeException('StubCursor does not map rows');
    }

    /**
     * @return null|array<string, mixed>
     */
    public function next(): ?array
    {
        return array_shift($this->rows);
    }
}
