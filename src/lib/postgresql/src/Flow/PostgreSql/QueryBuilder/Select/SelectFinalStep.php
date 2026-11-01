<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface SelectFinalStep extends Sql
{
    /**
     * Convert this SELECT query to a protobuf AST SelectStmt.
     */
    public function toAst(): SelectStmt;

    /**
     * Convert this SELECT query to SQL string.
     */
    public function toSql(): string;
}
