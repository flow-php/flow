<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface CreateMatViewAsStep extends CreateMatViewFinalStep
{
    public function as(SelectFinalStep $query) : CreateMatViewDataStep;
}
