<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface CreateTriggerOptionsStep extends CreateTriggerFinalStep
{
    public function deferrable(): self;

    public function execute(string $functionName, Expression ...$args): CreateTriggerFinalStep;

    public function forEachRow(): self;

    public function forEachStatement(): self;

    public function from(string $referencedTable): self;

    public function initiallyDeferred(): self;

    public function initiallyImmediate(): self;

    public function notDeferrable(): self;

    public function referencingNewTableAs(string $name): self;

    public function referencingOldTableAs(string $name): self;

    public function when(Condition $condition): self;
}
