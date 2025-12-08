<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateView;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface CreateViewAsStep extends CreateViewFinalStep
{
    public function as(SelectFinalStep $query) : CreateViewCheckOptionStep;
}
