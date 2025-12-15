<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\RuleStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateRuleFinalStep extends SqlQuery
{
    public function toAst() : RuleStmt;

    public function toSql() : string;
}
