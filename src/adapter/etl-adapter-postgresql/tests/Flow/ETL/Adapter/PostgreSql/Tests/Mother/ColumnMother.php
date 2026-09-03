<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Mother;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

use function Flow\PostgreSql\DSL\column_type_from_string;

/**
 * Builds the answer shape Client::describe() returns, from pg type names.
 */
final class ColumnMother
{
    /**
     * @param array<string, string> $nameToType
     *
     * @return list<array{name: string, type: ColumnType}>
     */
    public static function of(array $nameToType): array
    {
        $columns = [];

        foreach ($nameToType as $name => $type) {
            $columns[] = self::pair($name, $type);
        }

        return $columns;
    }

    /**
     * The primitive, so a projection with duplicate output names stays expressible.
     *
     * @return array{name: string, type: ColumnType}
     */
    public static function pair(string $name, string $type): array
    {
        return ['name' => $name, 'type' => column_type_from_string($type)];
    }
}
