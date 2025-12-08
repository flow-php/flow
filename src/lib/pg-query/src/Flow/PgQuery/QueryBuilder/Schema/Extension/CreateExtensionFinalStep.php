<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Extension;

use Flow\PgQuery\Protobuf\AST\CreateExtensionStmt;

interface CreateExtensionFinalStep
{
    public function toAst() : CreateExtensionStmt;
}
