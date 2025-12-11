<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;

interface TableConstraint
{
    public function toAst() : Constraint;
}
