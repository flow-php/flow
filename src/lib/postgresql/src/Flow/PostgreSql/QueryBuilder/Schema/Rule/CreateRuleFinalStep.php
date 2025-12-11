<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\RuleStmt;

interface CreateRuleFinalStep
{
    public function toAst() : RuleStmt;

    public function toSql() : string;
}
