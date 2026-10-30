<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\IO\Loader\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\IO\Loader\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\Values\Schema\TypesMap;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Values\Schema;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_key_exists;
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
        private TypesMap $typesMap,
    ) {}

    /**
     * @param array<string, mixed> $value encoded value map for a single row
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
            $params[] = $this->typesMap->map(
                $key,
                $schema->get((new UnresolvedReference($key))->name())->type(),
                $value[$key],
            );
        }

        return [delete()->from($this->table)->where(and_(...$conditions)), $params];
    }
}
