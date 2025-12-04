<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

use Flow\PgQuery\Protobuf\AST\InsertStmt;

/**
 * Terminal interface for INSERT query builder.
 */
interface InsertFinalStep
{
    /**
     * Convert this INSERT query to protobuf AST.
     */
    public function toAst() : InsertStmt;
}
