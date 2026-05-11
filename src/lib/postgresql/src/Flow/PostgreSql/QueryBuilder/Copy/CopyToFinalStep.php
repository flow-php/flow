<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

use Flow\PostgreSql\Protobuf\AST\CopyStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CopyToFinalStep extends Sql
{
    public function toAst(): CopyStmt;
}
