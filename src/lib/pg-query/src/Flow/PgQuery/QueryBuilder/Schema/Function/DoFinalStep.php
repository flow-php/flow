<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\DoStmt;

interface DoFinalStep
{
    public function language(string $language) : self;

    public function toAst() : DoStmt;

    public function toSql() : string;
}
