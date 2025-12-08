<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

use Flow\PgQuery\Protobuf\AST\AlterDomainStmt;

interface AlterDomainFinalStep
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : AlterDomainStmt;
}
