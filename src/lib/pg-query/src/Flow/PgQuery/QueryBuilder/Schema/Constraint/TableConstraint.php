<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Constraint;

use Flow\PgQuery\Protobuf\AST\Constraint;

interface TableConstraint
{
    public function toAst() : Constraint;
}
