<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView;

interface CreateMatViewDataStep extends CreateMatViewFinalStep
{
    public function tablespace(string $tablespace) : self;

    public function withData() : CreateMatViewFinalStep;

    public function withNoData() : CreateMatViewFinalStep;
}
