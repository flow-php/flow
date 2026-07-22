<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_key_exists;
use function Flow\ETL\DSL\ref;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\param;
use function sprintf;

final readonly class DeleteQueryBuilder
{
    public function __construct(
        private string $table,
        private EntryTypesMap $typesMap,
    ) {}

    /**
     * @param array<string, mixed> $value dehydrated value map for a single row
     *
     * @return array{Sql, list<null|TypedValue>}
     */
    public function build(array $value, Schema $schema, DeleteOptions $options): array
    {
        $primaryKeys = $options->primaryKeys;

        if ($primaryKeys === []) {
            throw new RuntimeException('Primary keys must be specified for DELETE operation');
        }

        $paramIndex = 1;
        $params = [];
        $conditions = [];

        foreach ($primaryKeys as $key) {
            if (!array_key_exists($key, $value)) {
                throw new RuntimeException(sprintf('Primary key "%s" not found in row', $key));
            }

            $conditions[] = eq(col($key), param($paramIndex++));
            $params[] = $this->typesMap->map($key, $schema->get(ref($key))->type(), $value[$key]);
        }

        $query = delete()->from($this->table)->where(and_(...$conditions));

        return [$query, $params];
    }
}
