<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

interface CreateRuleDoStep
{
    public function doAlso(string $command) : CreateRuleFinalStep;

    public function doInstead(string $command) : CreateRuleFinalStep;

    public function doNothing() : CreateRuleFinalStep;
}
