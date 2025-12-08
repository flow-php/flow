<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Extension;

use Flow\PgQuery\Protobuf\AST\{AlterExtensionContentsStmt, AlterExtensionStmt};

interface AlterExtensionFinalStep
{
    public function toAst() : AlterExtensionStmt|AlterExtensionContentsStmt;
}
