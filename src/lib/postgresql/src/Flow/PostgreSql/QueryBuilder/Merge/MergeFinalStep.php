<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\Protobuf\AST\MergeStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

/**
 * Terminal interface for MERGE query builder.
 */
interface MergeFinalStep extends Sql
{
    /**
     * Convert this MERGE query to protobuf AST.
     */
    public function toAst() : MergeStmt;

    public function toSql() : string;
}
