<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\CreateExtensionStmt;

interface CreateExtensionFinalStep
{
    public function toAst() : CreateExtensionStmt;

    public function toSql() : string;
}
