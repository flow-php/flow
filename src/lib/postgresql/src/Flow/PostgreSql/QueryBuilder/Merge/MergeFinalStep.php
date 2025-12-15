<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\Protobuf\AST\MergeStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

/**
 * Terminal interface for MERGE query builder.
 */
interface MergeFinalStep extends SqlQuery
{
    /**
     * Convert this MERGE query to protobuf AST.
     */
    public function toAst() : MergeStmt;

    public function toSql() : string;
}
