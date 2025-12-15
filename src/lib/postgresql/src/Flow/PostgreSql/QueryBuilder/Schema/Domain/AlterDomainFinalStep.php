<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\Protobuf\AST\AlterDomainStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterDomainFinalStep extends SqlQuery
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : AlterDomainStmt;

    public function toSql() : string;
}
