<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

interface InsertColumnsStep extends InsertValuesStep
{
    public function columns(string ...$columns): InsertValuesStep;
}
