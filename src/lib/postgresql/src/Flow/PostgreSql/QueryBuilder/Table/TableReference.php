<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;

/**
 * Base interface for all table references in FROM clause.
 *
 * @type ColumnAliases = array<string>
 */
interface TableReference extends AstConvertible
{
    /**
     * Create an aliased table reference: table AS alias (col1, col2).
     *
     * @param null|array<string> $columnAliases
     */
    public function as(string $alias, ?array $columnAliases = null): AliasedTable;
}
