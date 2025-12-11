<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\CreateTrigStmt;

interface CreateTriggerFinalStep
{
    public function toAst() : CreateTrigStmt;

    public function toSql() : string;
}
