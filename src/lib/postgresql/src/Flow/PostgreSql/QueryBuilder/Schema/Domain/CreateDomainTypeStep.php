<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\QueryBuilder\Schema\DataType;

interface CreateDomainTypeStep
{
    public function as(DataType $dataType) : CreateDomainOptionsStep;
}
