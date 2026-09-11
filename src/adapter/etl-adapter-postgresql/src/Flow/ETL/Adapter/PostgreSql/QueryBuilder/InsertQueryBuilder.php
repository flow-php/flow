<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_key_exists;
use function array_keys;
use function count;
use function Flow\ETL\DSL\ref;
use function Flow\PostgreSql\DSL\bulk_insert;
use function Flow\PostgreSql\DSL\conflict_columns;
use function Flow\PostgreSql\DSL\conflict_constraint;

final readonly class InsertQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * Every value already in PostgreSQL's text form. A column's converter is resolved once, on its first non-null
     * value - so a column of an unmapped type holding only nulls passes.
     *
     * @param list<array<string, mixed>> $values pre-sorted dehydrated value maps
     *
     * @return array{Sql, ConvertedParameters}
     */
    public function build(
        array $values,
        Schema $schema,
        ValueConverters $converters,
        ?InsertOptions $options = null,
    ): array {
        $columns = $values === [] ? [] : array_keys($values[0]);

        $params = [];
        $types = [];
        $columnConverters = [];

        // every row of a gated batch carries the same columns, so each column's type is resolved once
        foreach ($columns as $column) {
            $types[$column] = $schema->get(ref($column))->type();
        }

        foreach ($values as $row) {
            /** @var mixed $value */
            foreach ($row as $column => $value) {
                if ($value === null) {
                    $params[] = null;

                    continue;
                }

                if (!array_key_exists($column, $columnConverters)) {
                    $columnConverters[$column] = $converters->forValueType($this->typesMap->valueType(
                        $column,
                        $types[$column],
                    ));
                }

                $params[] = $columnConverters[$column]->toDatabase($value);
            }
        }

        return [$this->query($columns, count($values), $options), new ConvertedParameters($params)];
    }

    /**
     * @param list<string> $columns
     */
    private function query(array $columns, int $rows, ?InsertOptions $options): BulkInsert
    {
        $query = bulk_insert($this->table, $columns, $rows);

        if ($options !== null && $options->hasConflictHandling()) {
            return $this->applyConflictHandling($query, $options);
        }

        return $query;
    }

    private function applyConflictHandling(BulkInsert $query, InsertOptions $options): BulkInsert
    {
        if ($options->skipConflicts) {
            return $query->onConflictDoNothing();
        }

        $conflictTarget = null;

        if ($options->conflictColumns !== []) {
            $conflictTarget = conflict_columns($options->conflictColumns);
        } elseif ($options->conflictConstraint !== null) {
            $conflictTarget = conflict_constraint($options->conflictConstraint);
        }

        if ($conflictTarget === null) {
            return $query;
        }

        $updateColumns = $options->updateColumns !== [] ? $options->updateColumns : null;

        return $query->onConflictDoUpdate($conflictTarget, $updateColumns);
    }
}
