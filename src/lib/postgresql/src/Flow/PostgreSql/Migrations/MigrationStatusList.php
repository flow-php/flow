<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use ArrayIterator;
use Countable;
use IteratorAggregate;

use function array_filter;
use function array_values;
use function count;

/**
 * @implements \IteratorAggregate<int, MigrationStatus>
 */
final readonly class MigrationStatusList implements Countable, IteratorAggregate
{
    /**
     * @var list<MigrationStatus>
     */
    private array $statuses;

    public function __construct(MigrationStatus ...$statuses)
    {
        $this->statuses = array_values($statuses);
    }

    public function count(): int
    {
        return count($this->statuses);
    }

    public function executed(): self
    {
        return new self(...array_filter(
            $this->statuses,
            static fn(MigrationStatus $s): bool => $s->state === MigrationState::EXECUTED,
        ));
    }

    /**
     * @return \ArrayIterator<int, MigrationStatus>
     */
    public function getIterator(): ArrayIterator
    {
        return new ArrayIterator($this->statuses);
    }

    public function isEmpty(): bool
    {
        return $this->statuses === [];
    }

    public function pending(): self
    {
        return new self(...array_filter(
            $this->statuses,
            static fn(MigrationStatus $s): bool => $s->state === MigrationState::PENDING,
        ));
    }
}
