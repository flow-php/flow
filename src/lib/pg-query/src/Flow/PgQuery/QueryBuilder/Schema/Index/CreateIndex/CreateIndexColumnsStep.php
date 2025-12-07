<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex;

use Flow\PgQuery\QueryBuilder\Schema\Index\{IndexColumn, IndexMethod};

interface CreateIndexColumnsStep
{
    public function columns(IndexColumn|string ...$columns) : CreateIndexFinalStep;

    public function using(IndexMethod $method) : self;
}
