<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Delete;

use Flow\PgQuery\Protobuf\AST\DeleteStmt;

/**
 * Terminal interface for DELETE query builder.
 * Represents a complete DELETE statement that can be converted to AST.
 */
interface DeleteFinalStep
{
    /**
     * Convert this DELETE query to a protobuf AST DeleteStmt.
     */
    public function toAst() : DeleteStmt;

    /**
     * Convert this DELETE query to SQL string.
     */
    public function toSql() : string;
}
