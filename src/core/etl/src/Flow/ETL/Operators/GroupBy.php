<?php

declare(strict_types=1);

namespace Flow\ETL\Operators;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;
use SensitiveParameter;

use function array_fill;
use function array_key_exists;
use function array_values;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function iterator_to_array;

final readonly class GroupBy implements Describable
{
    /**
     * @throws RuntimeException when a pivot does not get exactly one aggregation
     */
    public function __construct(
        private References $refs,
        private Aggregators $aggregations = new Aggregators(),
        private ?Pivot $pivot = null,
    ) {
        if ($this->pivot !== null && $this->aggregations->count() !== 1) {
            throw new RuntimeException(
                'Pivot requires exactly one aggregation in group by, given: ' . $this->aggregations->count(),
            );
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $pivot = ($data['pivot'] ?? null) === null ? null : type_array()->assert($data['pivot']);

        return new self(
            $rebuild->of($data['refs'] ?? null, References::class),
            new Aggregators(...array_values($rebuild->all($data['aggregations'] ?? null, AggregatingFunction::class))),
            $pivot === null
                ? null
                : new Pivot(
                    $rebuild->of($pivot['column'] ?? null, Reference::class),
                    new DeclaredPivotValues(
                        ...type_list(type_union(type_integer(), type_string()))->assert($pivot['values'] ?? null),
                    ),
                ),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'refs' => $this->refs,
            'aggregations' => iterator_to_array($this->aggregations, false),
            'pivot' => $this->pivot === null
                ? null
                : ['column' => $this->pivot->column, 'values' => $this->pivot->values->all()],
        ]);
    }

    /**
     * The columns rows are grouped by.
     */
    public function refs(): References
    {
        return $this->refs;
    }

    public function requiredInputColumns(): RequiredColumns
    {
        $aggregated = $this->aggregations->references();

        if ($aggregated === null) {
            return RequiredColumns::all();
        }

        return RequiredColumns::only(...array_map(static fn(Reference $ref): string => $ref->base(), [
            ...$this->refs->all(),
            ...$aggregated,
            ...($this->pivot === null ? [] : [$this->pivot->column]),
        ]));
    }

    public function aggregations(): Aggregators
    {
        return $this->aggregations;
    }

    /**
     * @return null|list<string> the columns the grouping and every aggregation read; null when an aggregation does not
     *                           name the columns it reads
     */
    public function readColumns(): ?array
    {
        $aggregated = $this->aggregations->references();

        if ($aggregated === null) {
            return null;
        }

        $columns = [];

        foreach ([...$this->refs->all(), ...$aggregated] as $ref) {
            $columns[$ref->base()] = $ref->base();
        }

        return array_values($columns);
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
            $definition = $input->findDefinition($ref->to());

            if ($definition === null) {
                throw SchemaDefinitionNotFoundException::withAvailable($ref->to(), ...$input->names());
            }

            $definitions[] = $definition;
        }

        foreach ($bound as $aggregator) {
            $definitions[] = Definitions::of($aggregator->outputName(), $aggregator->returns());
        }

        return new Schema(...$definitions);
    }

    /**
     * @param list<int|string> $pivotColumns
     *
     * @throws InvalidArgumentException when a pivot value names a group-by column
     */
    public function pivotSchema(Schema $input, array $pivotColumns, AggregatingFunction $aggregation): Schema
    {
        $definitions = [];
        $refNames = [];

        foreach ($this->refs as $ref) {
            $definitions[] = $input->get($ref->to());
            $refNames[$ref->to()] = true;
        }

        foreach ($pivotColumns as $column) {
            $name = (string) $column;

            if (array_key_exists($name, $refNames)) {
                throw new InvalidArgumentException(
                    'Pivot value "' . $name . '" collides with the group-by column of the same name',
                );
            }

            // the yield loop writes null for a combination it never saw
            $definitions[] = Definitions::of($name, type_optional($aggregation->returns()));
        }

        return new Schema(...$definitions);
    }

    public function groupTable(Schema $input, Backend $backend): GroupTable
    {
        return $backend->groupTable($this->refs->names(), $input);
    }

    /**
     * @param list<int> $rowsOfGroups
     *
     * @return array<string, list<mixed>> the key values of those rows, by key column
     */
    public function keyValues(Rows $rows, Schema $input, array $rowsOfGroups): array
    {
        $values = [];

        foreach ($this->refs->all() as $ref) {
            // absent under a nullable declaration is a legitimate null; absent under NOT NULL is a row-shape
            // violation, and Rows::column() names it and lists the available columns.
            $values[$ref->to()] =
                $rowsOfGroups === []
                || $rows->schema()->findDefinition($ref->base()) === null && $input->get($ref->to())->isNullable()
                    ? array_fill(0, count($rowsOfGroups), null)
                    : $rows->column($ref->base())->take($rowsOfGroups)->values();
        }

        return $values;
    }

    public function pivotedBy(): ?Pivot
    {
        return $this->pivot;
    }
}
