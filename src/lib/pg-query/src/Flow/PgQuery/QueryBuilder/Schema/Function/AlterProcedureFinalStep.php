<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\{AlterFunctionStmt, RenameStmt};

interface AlterProcedureFinalStep
{
    public function toAlterAst() : AlterFunctionStmt;

    public function toRenameAst() : RenameStmt;
}
