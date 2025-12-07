<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

use Flow\PgQuery\Protobuf\AST\CopyStmt;

interface CopyFromFinalStep
{
    public function toAst() : CopyStmt;
}
