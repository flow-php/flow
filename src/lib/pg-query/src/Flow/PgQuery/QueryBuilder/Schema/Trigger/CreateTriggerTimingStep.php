<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

interface CreateTriggerTimingStep
{
    public function after(TriggerEvent ...$events) : CreateTriggerOnStep;

    public function afterUpdateOf(string ...$columns) : CreateTriggerOnStep;

    public function before(TriggerEvent ...$events) : CreateTriggerOnStep;

    public function beforeUpdateOf(string ...$columns) : CreateTriggerOnStep;

    public function constraint() : self;

    public function insteadOf(TriggerEvent ...$events) : CreateTriggerOnStep;

    public function orReplace() : self;
}
