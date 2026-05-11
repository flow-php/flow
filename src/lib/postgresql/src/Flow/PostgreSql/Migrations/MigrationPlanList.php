<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

/**
 * @implements \IteratorAggregate<int, MigrationPlan>
 */
final readonly class MigrationPlanList implements \Countable, \IteratorAggregate
{
    /**
     * @var list<MigrationPlan>
     */
    private array $plans;

    public function __construct(MigrationPlan ...$plans)
    {
        $this->plans = \array_values($plans);
    }

    public function count(): int
    {
        return \count($this->plans);
    }

    /**
     * @return \ArrayIterator<int, MigrationPlan>
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->plans);
    }

    public function isEmpty(): bool
    {
        return $this->plans === [];
    }
}
