<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\CompositeTypeStmt;

interface CreateCompositeTypeFinalStep
{
    public function toAst() : CompositeTypeStmt;
}
