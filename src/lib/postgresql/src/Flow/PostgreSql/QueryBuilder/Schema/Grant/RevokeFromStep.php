<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

interface RevokeFromStep
{
    public function from(string ...$roles) : RevokeFinalStep;

    public function fromPublic() : RevokeFinalStep;
}
