<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

interface CreateRangeTypeSubtypeStep
{
    public function subtype(string $type): CreateRangeTypeOptionsStep;
}
