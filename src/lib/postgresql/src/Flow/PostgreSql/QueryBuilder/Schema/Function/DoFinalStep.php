<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\DoStmt;

interface DoFinalStep
{
    public function language(string $language) : self;

    public function toAst() : DoStmt;

    public function toSql() : string;
}
