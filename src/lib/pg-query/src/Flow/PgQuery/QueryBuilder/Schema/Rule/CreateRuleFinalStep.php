<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Rule;

use Flow\PgQuery\Protobuf\AST\RuleStmt;

interface CreateRuleFinalStep
{
    public function toAst() : RuleStmt;

    public function toSql() : string;
}
