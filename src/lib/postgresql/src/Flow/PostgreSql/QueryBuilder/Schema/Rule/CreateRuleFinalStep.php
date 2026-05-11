<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\RuleStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateRuleFinalStep extends Sql
{
    public function toAst(): RuleStmt;

    public function toSql(): string;
}
