<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Condition\Condition;
use Flow\PgQuery\QueryBuilder\Table\TableReference;

interface SelectJoinStep extends SelectWhereStep
{
    public function crossJoin(TableReference $table) : self;

    public function fullJoin(TableReference $table, Condition $on) : self;

    public function join(TableReference $table, Condition $on) : self;

    public function leftJoin(TableReference $table, Condition $on) : self;

    public function rightJoin(TableReference $table, Condition $on) : self;
}
