<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\DoStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DoFinalStep extends Sql
{
    public function language(string $language) : self;

    public function toAst() : DoStmt;

    public function toSql() : string;
}
