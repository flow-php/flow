<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateView;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface CreateViewOptionsStep extends CreateViewFinalStep
{
    public function as(SelectFinalStep $query) : CreateViewCheckOptionStep;

    public function columns(string ...$columns) : CreateViewAsStep;

    public function orReplace() : self;

    public function recursive() : self;

    public function temporary() : self;
}
