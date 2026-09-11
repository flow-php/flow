<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Schema\Definition;

final readonly class ValueMatch
{
    /**
     * @param Definition<mixed> $definition
     */
    public function matches(Definition $definition, mixed $value): bool
    {
        if ($value === null) {
            return $definition->isNullable();
        }

        return $definition->type()->isValid($value);
    }
}
