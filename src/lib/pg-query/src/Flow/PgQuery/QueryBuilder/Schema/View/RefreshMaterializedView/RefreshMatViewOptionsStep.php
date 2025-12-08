<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\RefreshMaterializedView;

interface RefreshMatViewOptionsStep extends RefreshMatViewFinalStep
{
    public function concurrently() : RefreshMatViewFinalStep;
}
