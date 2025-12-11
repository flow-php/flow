<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\{AlterExtensionContentsStmt, AlterExtensionStmt};

interface AlterExtensionFinalStep
{
    public function toAst() : AlterExtensionStmt|AlterExtensionContentsStmt;

    public function toSql() : string;
}
