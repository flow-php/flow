<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Ownership;

interface ReassignOwnedToStep
{
    public function to(string $newRole) : ReassignOwnedFinalStep;
}
