<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\Protobuf\AST\SelectStmt;

/**
 * Terminal interface for SELECT query builder.
 * Represents a complete SELECT statement that can be converted to AST.
 */
interface SelectFinalStep
{
    /**
     * Convert this SELECT query to a protobuf AST SelectStmt.
     */
    public function toAst() : SelectStmt;

    /**
     * Convert this SELECT query to SQL string.
     */
    public function toSql() : string;
}
