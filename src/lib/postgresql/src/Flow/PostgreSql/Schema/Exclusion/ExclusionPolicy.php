<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

interface ExclusionPolicy
{
    public function exclude(SchemaObject $object): bool;
}
