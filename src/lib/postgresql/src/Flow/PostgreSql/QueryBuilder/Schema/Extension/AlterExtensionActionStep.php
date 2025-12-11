<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

interface AlterExtensionActionStep
{
    public function addFunction(string $function) : AlterExtensionFinalStep;

    public function addTable(string $table) : AlterExtensionFinalStep;

    public function dropFunction(string $function) : AlterExtensionFinalStep;

    public function dropTable(string $table) : AlterExtensionFinalStep;

    public function update() : AlterExtensionFinalStep;

    public function updateTo(string $version) : AlterExtensionFinalStep;
}
