<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

interface CreateDomainTypeStep
{
    public function as(string $dataType) : CreateDomainOptionsStep;
}
