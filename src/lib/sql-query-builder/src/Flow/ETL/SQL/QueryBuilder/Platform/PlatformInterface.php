<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

/**
 * Interface for database platform-specific SQL generation.
 */
interface PlatformInterface
{
    /**
     * Build SQL query from query parts.
     *
     * @param array{
     *     ctes: array<string, array{query: string, columns: array<string>, recursive: bool}>,
     *     select: array<string>,
     *     from: array{table: string, alias: string|null}|null,
     *     joins: array<array{type: string, table: string, alias: string, condition: string|null, fromAlias: string}>,
     *     where: array<string>,
     *     groupBy: array<string>,
     *     having: array<string>,
     *     orderBy: array<array{column: string, direction: string}>,
     *     limit: int|null,
     *     offset: int|null
     * } $queryParts
     * @return string
     */
    public function buildSQL(array $queryParts): string;

    /**
     * Quote an identifier (table or column name).
     *
     * @param string $identifier
     * @return string
     */
    public function quoteIdentifier(string $identifier): string;

    /**
     * Convert a value to a SQL literal.
     *
     * @param mixed $value
     * @return string
     */
    public function literal(mixed $value): string;

    /**
     * Check if the platform supports Common Table Expressions.
     *
     * @return bool
     */
    public function supportsCTE(): bool;

    /**
     * Check if the platform supports recursive CTEs.
     *
     * @return bool
     */
    public function supportsRecursiveCTE(): bool;

    /**
     * Check if the platform supports LATERAL joins.
     *
     * @return bool
     */
    public function supportsLateralJoins(): bool;

    /**
     * Get the platform name.
     *
     * @return string
     */
    public function getName(): string;
}