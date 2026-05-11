<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

interface AlterProcedureArgsStep extends AlterProcedureFinalStep
{
    public function arguments(FunctionArgument ...$args): AlterProcedureFinalStep;

    public function renameTo(string $newName): AlterProcedureFinalStep;

    public function reset(string $parameter): AlterProcedureFinalStep;

    public function resetAll(): AlterProcedureFinalStep;

    public function securityDefiner(): AlterProcedureFinalStep;

    public function securityInvoker(): AlterProcedureFinalStep;

    public function set(string $parameter, string $value): AlterProcedureFinalStep;
}
