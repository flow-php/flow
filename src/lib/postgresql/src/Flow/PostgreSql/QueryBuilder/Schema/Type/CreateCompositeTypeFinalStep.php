<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CompositeTypeStmt;

interface CreateCompositeTypeFinalStep
{
    public function toAst() : CompositeTypeStmt;

    public function toSql() : string;
}
