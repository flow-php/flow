<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
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

    public function accumulate(Rows $batch, FlowContext $context): void
    {
        /** @var array<string, list<int>> $indices */
        $indices = [];

        /** @var array<string, GroupKey> $keys */
        $keys = [];

        foreach ($this->groupBy->keys($batch, $this->shape->input) as $i => $key) {
            $index = (string) $key;
            $indices[$index][] = $i;
            $keys[$index] ??= $key;
        }

        foreach ($indices as $index => $rows) {
            $group = $this->groups[$index] ??= new Group($keys[$index], $this->shape->aggregators->cloned());
            $group->aggregators->aggregate($batch, $rows, $context);
        }
    }

    /**
     * $group aggregated rows that come before every row this instance aggregated under the same key.
     */
    public function absorbEarlier(string $key, Group $group, FlowContext $context): void
    {
        if (array_key_exists($key, $this->groups)) {
            $group->aggregators->merge($this->groups[$key]->aggregators, $context);
        }

        $this->groups[$key] = $group;
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<Rows>
     */
    public function flush(int $batchSize, FlowContext $context): Generator
    {
        foreach (array_chunk($this->groups, $batchSize) as $groups) {
            yield $this->rows($groups, $context);
        }
    }

    /**
     * @param list<Group> $groups
     */
    public function rows(array $groups, FlowContext $context): Rows
    {
        $output = $this->shape->output;
        $values = [];

        foreach ($output->definitions() as $name => $_) {
            $values[$name] = [];
        }

        foreach ($groups as $group) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($group->key as $name => $value) {
                $values[$output->get($name)->entry()->name()][] = $value;
            }

            foreach ($group->aggregators as $aggregator) {
                $values[$aggregator->outputName()][] = $aggregator->value();
            }
        }

        $columns = [];

        foreach ($output->definitions() as $name => $definition) {
            $builder = $context->backend()->builder($definition);
            $builder->appendMany($values[$name]);
            $columns[$name] = $builder->finish();
        }

        return Rows::fromColumns($output, $columns, count($groups));
    }
}
