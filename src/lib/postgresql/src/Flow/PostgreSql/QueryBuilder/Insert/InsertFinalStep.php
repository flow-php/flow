<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

/**
 * Terminal interface for INSERT query builder.
 */
interface InsertFinalStep extends Sql
{
    /**
     * Convert this INSERT query to protobuf AST.
     */
    public function toAst(): InsertStmt;

    /**
     * Convert this INSERT query to SQL string.
     */
    public function toSql(): string;
}
