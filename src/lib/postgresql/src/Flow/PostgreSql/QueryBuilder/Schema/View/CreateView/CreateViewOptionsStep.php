<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface CreateViewOptionsStep extends CreateViewFinalStep
{
    public function as(SelectFinalStep $query): CreateViewCheckOptionStep;

    public function columns(string ...$columns): CreateViewAsStep;

    public function orReplace(): self;

    public function temporary(): self;
}
