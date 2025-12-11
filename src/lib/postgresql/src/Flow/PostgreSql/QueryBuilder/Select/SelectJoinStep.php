<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Table\TableReference;

interface SelectJoinStep extends SelectWhereStep
{
    public function crossJoin(TableReference $table) : self;

    public function fullJoin(TableReference $table, Condition $on) : self;

    public function join(TableReference $table, Condition $on) : self;

    public function leftJoin(TableReference $table, Condition $on) : self;

    public function rightJoin(TableReference $table, Condition $on) : self;
}
