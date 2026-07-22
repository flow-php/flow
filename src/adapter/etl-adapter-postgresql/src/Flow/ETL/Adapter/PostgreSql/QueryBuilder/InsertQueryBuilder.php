<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use Flow\PostgreSql\QueryBuilder\Sql;

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
     * @param list<array<string, mixed>> $values pre-sorted dehydrated value maps
     *
     * @return array{Sql, list<null|TypedValue>}
     */
    public function build(array $values, Schema $schema, ?InsertOptions $options = null): array
    {
        $columns = $values === [] ? [] : array_keys($values[0]);

        $params = [];

        foreach ($values as $row) {
            /** @var mixed $value */
            foreach ($row as $column => $value) {
                $params[] = $this->typesMap->map($column, $schema->get(ref($column))->type(), $value);
            }
        }

        $query = bulk_insert($this->table, $columns, count($values));

        if ($options !== null && $options->hasConflictHandling()) {
            $query = $this->applyConflictHandling($query, $options);
        }

        return [$query, $params];
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
