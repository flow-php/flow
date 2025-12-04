<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

use Flow\PgQuery\QueryBuilder\Expression\Expression;
use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface InsertValuesStep extends InsertOnConflictStep
{
    public function defaultValues() : InsertOnConflictStep;

    public function select(SelectFinalStep $select) : InsertOnConflictStep;

    public function values(Expression ...$values) : self;
}
