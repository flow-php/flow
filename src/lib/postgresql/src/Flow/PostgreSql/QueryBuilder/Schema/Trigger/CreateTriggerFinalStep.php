<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\CreateTrigStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateTriggerFinalStep extends SqlQuery
{
    public function toAst() : CreateTrigStmt;

    public function toSql() : string;
}
