<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

interface GrantToStep
{
    public function to(string ...$roles) : GrantFinalStep;

    public function toPublic() : GrantFinalStep;
}
