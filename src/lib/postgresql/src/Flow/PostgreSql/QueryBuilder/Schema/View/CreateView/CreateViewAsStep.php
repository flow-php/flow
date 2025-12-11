<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface CreateViewAsStep extends CreateViewFinalStep
{
    public function as(SelectFinalStep $query) : CreateViewCheckOptionStep;
}
