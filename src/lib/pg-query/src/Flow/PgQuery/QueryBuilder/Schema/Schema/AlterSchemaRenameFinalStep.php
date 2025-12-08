<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Schema;

use Flow\PgQuery\Protobuf\AST\RenameStmt;

interface AlterSchemaRenameFinalStep
{
    public function toAst() : RenameStmt;
}
