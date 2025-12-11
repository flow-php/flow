<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Update;

use Flow\PostgreSql\Protobuf\AST\UpdateStmt;

/**
 * Terminal interface for UPDATE query builder.
 */
interface UpdateFinalStep
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
