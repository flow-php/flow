<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\CreateIndex;

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexColumn;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod;

interface CreateIndexColumnsStep
{
    public function columns(IndexColumn|string ...$columns): CreateIndexFinalStep;

    public function using(IndexMethod $method): self;
}
