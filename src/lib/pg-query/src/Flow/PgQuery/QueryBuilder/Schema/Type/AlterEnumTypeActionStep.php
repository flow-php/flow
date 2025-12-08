<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

interface AlterEnumTypeActionStep
{
    public function addValue(string $value) : AlterEnumTypeFinalStep;

    public function addValueAfter(string $value, string $neighbor) : AlterEnumTypeFinalStep;

    public function addValueBefore(string $value, string $neighbor) : AlterEnumTypeFinalStep;

    public function renameValue(string $oldValue, string $newValue) : AlterEnumTypeFinalStep;
}
