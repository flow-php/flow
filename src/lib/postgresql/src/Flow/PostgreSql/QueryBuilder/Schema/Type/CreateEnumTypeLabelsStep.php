<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

interface CreateEnumTypeLabelsStep
{
    public function labels(string ...$labels): CreateEnumTypeFinalStep;
}
