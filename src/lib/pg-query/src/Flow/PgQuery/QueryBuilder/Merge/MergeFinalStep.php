<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\Protobuf\AST\MergeStmt;

/**
 * Terminal interface for MERGE query builder.
 */
interface MergeFinalStep
{
    /**
     * Convert this MERGE query to protobuf AST.
     */
    public function toAst() : MergeStmt;

    public function toSql() : string;
}
