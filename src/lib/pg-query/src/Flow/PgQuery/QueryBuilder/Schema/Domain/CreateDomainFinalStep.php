<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

use Flow\PgQuery\Protobuf\AST\CreateDomainStmt;

interface CreateDomainFinalStep
{
    public function toAst() : CreateDomainStmt;
}
