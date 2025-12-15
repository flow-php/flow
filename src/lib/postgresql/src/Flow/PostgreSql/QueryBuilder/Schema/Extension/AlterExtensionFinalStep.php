<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\{AlterExtensionContentsStmt, AlterExtensionStmt};
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterExtensionFinalStep extends SqlQuery
{
    public function toAst() : AlterExtensionStmt|AlterExtensionContentsStmt;

    public function toSql() : string;
}
