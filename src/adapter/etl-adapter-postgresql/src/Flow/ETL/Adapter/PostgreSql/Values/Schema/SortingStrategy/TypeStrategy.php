<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Values\Schema\SortingStrategy;

use Flow\ETL\Adapter\PostgreSql\Foundation\Exception\TypeMappingException;
use Flow\ETL\Adapter\PostgreSql\Values\Schema\PostgreSqlMetadata;
use Flow\ETL\Adapter\PostgreSql\Values\Schema\TypesMap;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\SortingStrategy;

use function Flow\Types\DSL\type_string;

final readonly class TypeStrategy implements SortingStrategy
{
    public function __construct(
        private TypesMap $typesMap = new TypesMap(),
    ) {}

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int
    {
        $leftPrimaryKey = $left->metadata()->has(PostgreSqlMetadata::PRIMARY_KEY->value);

        if ($leftPrimaryKey !== $right->metadata()->has(PostgreSqlMetadata::PRIMARY_KEY->value)) {
            return $leftPrimaryKey ? -1 : 1;
        }

        $typeComparison = $this->postgreSqlType($left) <=> $this->postgreSqlType($right);

        if ($typeComparison !== 0) {
            return $typeComparison;
        }

        return $left->name() <=> $right->name();
    }

    /**
     * @param Definition<mixed> $definition
     */
    private function postgreSqlType(Definition $definition): string
    {
        $metadata = $definition->metadata();

        if ($metadata->has(PostgreSqlMetadata::TYPE->value)) {
            return (string) $metadata->getAs(PostgreSqlMetadata::TYPE->value, type_string());
        }

        try {
            return $this->typesMap->toColumnType($definition->type())->normalize()['name'];
        } catch (TypeMappingException) {
            return "\xff" . $definition->type()::class;
        }
    }
}
