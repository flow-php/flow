<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy\Aggregators;
use Flow\ETL\GroupBy\DeclaredPivotValues;
use Flow\ETL\GroupBy\GroupKey;
use Flow\ETL\GroupBy\Pivot;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;

use function array_unique;
use function count;
use function Flow\ETL\DSL\definition_from_type;

final class GroupBy
{
    private Aggregators $aggregations;

    private ?Pivot $pivot = null;

    private readonly References $refs;

    public function __construct(string|Reference ...$entries)
    {
        $this->refs = References::init(...array_unique($entries));
        $this->aggregations = new Aggregators();
    }

    public function aggregate(AggregatingFunction ...$aggregator): void
    {
        if (!count($aggregator)) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        if ($this->pivot !== null && count($aggregator) !== 1) {
            throw new RuntimeException(
                'Pivot requires exactly one aggregation in group by, given: ' . count($aggregator),
            );
        }

        $this->aggregations = new Aggregators(...$aggregator);
    }

    public function aggregatedRow(GroupKey $key, Aggregators $aggregators, Schema $output): Row
    {
        $values = [];

        /** @var mixed $value */
        foreach ($key as $name => $value) {
            $values[$name] = $value === null ? null : $output->get($name)->type()->cast($value);
        }

        foreach ($aggregators as $aggregator) {
            $definition = $output->get($aggregator->outputName());
            /** @var mixed $value */
            $value = $aggregator->value();
            $values[$aggregator->outputName()] = $value === null ? null : $definition->type()->cast($value);
        }

        return new Row($values);
    }

    public function aggregations(): Aggregators
    {
        return $this->aggregations;
    }

    public function isGlobal(): bool
    {
        return $this->refs->count() === 0;
    }

    public function isPivot(): bool
    {
        return $this->pivot !== null;
    }

    /**
     * The aggregate operator's declared output schema, computed once per run. Group-key definitions
     * come from the input schema unchanged: a key is null only when the input column was already
     * nullable. A key the batch schema does not declare refuses at bind - inventing a column would
     * put a value in row storage that no schema-driven reader can see.
     */
    public function outputSchema(Schema $input, Aggregators $bound): Schema
    {
        $definitions = [];

        foreach ($this->refs as $ref) {
            $definition = $input->findDefinition($ref);

            if ($definition === null) {
                throw SchemaDefinitionNotFoundException::withAvailable($ref->name(), ...$input->references()->names());
            }

            $definitions[] = $definition;
        }

        foreach ($bound as $aggregator) {
            $definitions[] = definition_from_type($aggregator->outputName(), $aggregator->returns());
        }

        return new Schema(...$definitions);
    }

    public function keyValues(Row $row, Schema $input): GroupKey
    {
        $values = [];

        foreach ($this->refs as $ref) {
            // absent under a nullable declaration is a legitimate null; absent under NOT NULL is a
            // row-shape violation, and Row::get() already names it and lists the available columns.
            $values[$ref->name()] = !$row->has($ref) && $input->get($ref)->isNullable() ? null : $row->get($ref);
        }

        return new GroupKey($values);
    }

    public function pivot(Reference $ref, DeclaredPivotValues $values): void
    {
        $this->pivot = new Pivot($ref, $values);
    }

    public function pivotedBy(): ?Pivot
    {
        return $this->pivot;
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return $this->refs->all();
    }
}
