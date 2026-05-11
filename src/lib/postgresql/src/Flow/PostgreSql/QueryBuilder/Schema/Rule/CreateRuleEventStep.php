<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

interface CreateRuleEventStep
{
    public function asOnDelete(): CreateRuleToStep;

    public function asOnInsert(): CreateRuleToStep;

    public function asOnSelect(): CreateRuleToStep;

    public function asOnUpdate(): CreateRuleToStep;

    public function orReplace(): self;
}
