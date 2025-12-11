<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\RenameStmt;

interface RenameSequenceFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
