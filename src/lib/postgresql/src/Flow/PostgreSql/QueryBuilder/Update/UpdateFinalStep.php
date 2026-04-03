<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Update;

use Flow\PostgreSql\Protobuf\AST\UpdateStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

/**
 * Terminal interface for UPDATE query builder.
 */
interface UpdateFinalStep extends Sql
{
    /**
     * Convert this UPDATE query to protobuf AST.
     */
    public function toAst() : UpdateStmt;

    /**
     * Convert this UPDATE query to SQL string.
     */
    public function toSql() : string;
}
