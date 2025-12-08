<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

interface CreateRangeTypeSubtypeStep
{
    public function subtype(string $type) : CreateRangeTypeOptionsStep;
}
