<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Exception\InvalidArgumentException;
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
use function sprintf;

final readonly class InsertQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * Every value already in PostgreSQL's text form. A column's converter is resolved once, on its first non-null
     * value - so a column of an unmapped type holding only nulls passes. Parameters are bound row by row, read
     * straight from the columns.
     *
     * @param array<string, list<mixed>> $columns every column's encoded values, $count of them each
     *
     * @throws InvalidArgumentException a column that does not hold $count values
     *
     * @return array{Sql, ConvertedParameters}
     */
    public function build(
        array $columns,
        int $count,
        Schema $schema,
        ValueConverters $converters,
        ?InsertOptions $options = null,
    ): array {
        foreach ($columns as $name => $values) {
            if (count($values) !== $count) {
                throw new InvalidArgumentException(sprintf(
                    'Column "%s" holds %d values, the insert %d rows',
                    $name,
                    count($values),
                    $count,
                ));
            }
        }

        $names = $count === 0 ? [] : array_keys($columns);
        $params = [];
        $types = [];
        $columnConverters = [];

        foreach ($names as $column) {
            $types[$column] = $schema->get(ref($column))->type();
        }

        for ($i = 0; $i < $count; $i++) {
            foreach ($names as $column) {
                // @mago-ignore analysis:mixed-assignment
                $value = $columns[$column][$i];

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

        return [$this->query($names, $count, $options), new ConvertedParameters($params)];
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
