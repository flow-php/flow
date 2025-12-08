<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

interface CreateProcedureArgsStep extends CreateProcedureOptionsStep
{
    public function arguments(FunctionArgument ...$args) : CreateProcedureOptionsStep;

    public function orReplace() : self;
}
