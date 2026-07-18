<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_key_exists;
use function Flow\ETL\DSL\ref;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\update;
use function in_array;
use function sprintf;

final readonly class UpdateQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * @param array<string, mixed> $value dehydrated value map for a single row
     *
     * @return array{null|Sql, list<null|TypedValue>}
     */
    public function build(array $value, Schema $schema, UpdateOptions $options): array
    {
        $primaryKeys = $options->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for UPDATE operation');
        }

        $paramIndex = 1;
        $params = [];
        $assignments = [];

        /** @var mixed $columnValue */
        foreach ($value as $column => $columnValue) {
            if (in_array($column, $primaryKeys, true)) {
                continue;
            }

            $assignments[$column] = param($paramIndex++);
            $params[] = $this->typesMap->map($column, $schema->get(ref($column))->type(), $columnValue);
        }

        if ($assignments === []) {
            return [null, []];
        }

        $conditions = [];

        foreach ($primaryKeys as $key) {
            if (!array_key_exists($key, $value)) {
                throw new RuntimeException(sprintf('Primary key "%s" not found in row', $key));
            }

            $conditions[] = eq(col($key), param($paramIndex++));
            $params[] = $this->typesMap->map($key, $schema->get(ref($key))->type(), $value[$key]);
        }

        $query = update()->update($this->table)->setAll($assignments)->where(and_(...$conditions));

        return [$query, $params];
    }
}
