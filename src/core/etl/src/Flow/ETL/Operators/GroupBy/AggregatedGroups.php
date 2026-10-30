<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;

use function array_chunk;
use function array_key_exists;
use function count;

final class AggregatedGroups
{
    /**
     * @var array<string, Group>
     */
    private array $groups = [];

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly GroupByShape $shape,
    ) {}

    /**
     * @return array<string, Group> by the key's string form
     */
    public function groups(): array
    {
        return $this->groups;
    }

    public function accumulate(Rows $batch, Backend $backend): void
    {
        foreach ($this->groupBy->grouped($batch, $this->shape->input) as $identity => $rows) {
            ($this->groups[$identity] ??= new Group(
                $this->groupBy->key($batch, $this->shape->input, $rows[0], $identity),
                $this->shape->aggregators->fresh(),
            ))->aggregators->aggregate($batch, $rows, $backend);
        }
    }

    /**
     * $group aggregated rows that come before every row this instance aggregated under the same key.
     */
    public function absorbEarlier(string $key, Group $group, Backend $backend): void
    {
        if (array_key_exists($key, $this->groups)) {
            $group->aggregators->merge($this->groups[$key]->aggregators, $backend);
        }

        $this->groups[$key] = $group;
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<Rows>
     */
    public function flush(int $batchSize, Backend $backend): Generator
    {
        foreach (array_chunk($this->groups, $batchSize) as $groups) {
            yield $this->rows($groups, $backend);
        }
    }

    /**
     * @param list<Group> $groups
     */
    public function rows(array $groups, Backend $backend): Rows
    {
        $output = $this->shape->output;
        $values = [];

        foreach ($output->definitions() as $name => $_) {
            $values[$name] = [];
        }

        foreach ($groups as $group) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($group->key as $name => $value) {
                $values[$output->get($name)->name()][] = $value;
            }

            foreach ($group->aggregators as $aggregator) {
                $values[$aggregator->outputName()][] = $aggregator->value();
            }
        }

        $columns = [];

        foreach ($output->definitions() as $name => $definition) {
            $builder = $backend->builder($definition);
            $builder->appendMany($values[$name]);
            $columns[$name] = $builder->finish();
        }

        return Rows::fromColumns($output, $columns, count($groups));
    }
}
