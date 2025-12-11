<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

use Flow\PostgreSql\Protobuf\AST\CopyStmt;

interface CopyFromFinalStep
{
    public function toAst() : CopyStmt;
}
