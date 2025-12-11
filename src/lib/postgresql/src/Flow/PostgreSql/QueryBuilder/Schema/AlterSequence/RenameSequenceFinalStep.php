<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;

interface RenameSequenceFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
