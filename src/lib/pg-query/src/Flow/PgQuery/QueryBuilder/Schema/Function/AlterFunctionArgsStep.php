<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

interface AlterFunctionArgsStep extends AlterFunctionFinalStep
{
    public function arguments(FunctionArgument ...$args) : AlterFunctionFinalStep;

    public function cost(int $cost) : AlterFunctionFinalStep;

    public function immutable() : AlterFunctionFinalStep;

    public function parallel(ParallelSafety $safety) : AlterFunctionFinalStep;

    public function renameTo(string $newName) : AlterFunctionFinalStep;

    public function reset(string $parameter) : AlterFunctionFinalStep;

    public function resetAll() : AlterFunctionFinalStep;

    public function rows(int $rows) : AlterFunctionFinalStep;

    public function set(string $parameter, string $value) : AlterFunctionFinalStep;

    public function stable() : AlterFunctionFinalStep;

    public function volatile() : AlterFunctionFinalStep;
}
