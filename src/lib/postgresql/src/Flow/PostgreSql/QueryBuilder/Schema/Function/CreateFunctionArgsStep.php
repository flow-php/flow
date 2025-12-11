<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

interface CreateFunctionArgsStep extends CreateFunctionReturnsStep
{
    public function arguments(FunctionArgument ...$args) : CreateFunctionReturnsStep;

    public function orReplace() : self;
}
