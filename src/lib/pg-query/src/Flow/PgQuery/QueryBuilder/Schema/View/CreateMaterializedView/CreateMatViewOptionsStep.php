<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface CreateMatViewOptionsStep extends CreateMatViewFinalStep
{
    public function as(SelectFinalStep $query) : CreateMatViewDataStep;

    public function columns(string ...$columns) : CreateMatViewAsStep;

    public function ifNotExists() : self;

    public function using(string $method) : self;
}
