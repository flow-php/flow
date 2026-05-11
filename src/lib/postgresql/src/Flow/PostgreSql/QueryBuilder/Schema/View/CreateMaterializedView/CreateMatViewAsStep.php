<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface CreateMatViewAsStep extends CreateMatViewFinalStep
{
    public function as(SelectFinalStep $query): CreateMatViewDataStep;
}
