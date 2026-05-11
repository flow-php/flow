<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

interface AlterTriggerActionStep
{
    public function dependsOnExtension(string $extension): AlterTriggerFinalStep;

    public function noDependsOnExtension(string $extension): AlterTriggerFinalStep;

    public function renameTo(string $newName): AlterTriggerFinalStep;
}
