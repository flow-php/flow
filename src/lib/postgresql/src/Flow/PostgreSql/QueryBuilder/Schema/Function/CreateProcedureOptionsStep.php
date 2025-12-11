<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

interface CreateProcedureOptionsStep extends CreateProcedureFinalStep
{
    public function as(string $definition) : CreateProcedureFinalStep;

    public function language(string $language) : self;

    public function securityDefiner() : self;

    public function securityInvoker() : self;

    public function set(string $parameter, string $value) : self;
}
