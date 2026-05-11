<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

interface CreateDomainTypeStep
{
    public function as(ColumnType $dataType): CreateDomainOptionsStep;
}
