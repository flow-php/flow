<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\CreateTrigStmt;

interface CreateTriggerFinalStep
{
    public function toAst() : CreateTrigStmt;
}
