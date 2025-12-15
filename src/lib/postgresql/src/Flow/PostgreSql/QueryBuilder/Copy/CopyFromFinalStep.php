<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

use Flow\PostgreSql\Protobuf\AST\CopyStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CopyFromFinalStep extends SqlQuery
{
    public function toAst() : CopyStmt;
}
