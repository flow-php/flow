<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\Protobuf\AST\AlterDomainStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterDomainFinalStep extends Sql
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : AlterDomainStmt;

    public function toSql() : string;
}
