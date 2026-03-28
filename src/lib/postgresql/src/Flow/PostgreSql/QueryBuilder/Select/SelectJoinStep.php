<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Table\TableReference;

interface SelectJoinStep extends SelectWhereStep
{
    public function crossJoin(string|TableReference $table) : self;

    public function fullJoin(string|TableReference $table, Condition $on) : self;

    public function join(string|TableReference $table, Condition $on) : self;

    public function leftJoin(string|TableReference $table, Condition $on) : self;

    public function rightJoin(string|TableReference $table, Condition $on) : self;
}
