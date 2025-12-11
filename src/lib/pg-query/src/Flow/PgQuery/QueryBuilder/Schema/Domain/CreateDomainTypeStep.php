<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

use Flow\PgQuery\QueryBuilder\Schema\DataType;

interface CreateDomainTypeStep
{
    public function as(DataType $dataType) : CreateDomainOptionsStep;
}
