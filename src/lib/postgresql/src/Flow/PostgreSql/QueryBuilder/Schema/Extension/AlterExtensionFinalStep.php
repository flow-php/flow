<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\AlterExtensionContentsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterExtensionFinalStep extends Sql
{
    public function toAst(): AlterExtensionStmt|AlterExtensionContentsStmt;

    public function toSql(): string;
}
