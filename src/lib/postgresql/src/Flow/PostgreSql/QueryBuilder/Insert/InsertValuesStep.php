<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface InsertValuesStep extends InsertOnConflictStep
{
    public function defaultValues(): InsertOnConflictStep;

    public function select(SelectFinalStep $select): InsertOnConflictStep;

    public function values(Expression ...$values): self;
}
