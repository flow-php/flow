<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

use Flow\PgQuery\Protobuf\AST\UpdateStmt;

/**
 * Terminal interface for UPDATE query builder.
 */
interface UpdateFinalStep
{
    /**
     * Convert this UPDATE query to protobuf AST.
     */
    public function toAst() : UpdateStmt;
}
