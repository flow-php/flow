<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Optional implements Expression
{
    use Row\Reference\EntryExpression;

    public function __construct(private readonly Expression $expression)
    {
    }

    public function eval(Row $row) : mixed
    {
        try {
            return $this->expression->eval($row);
        } catch (\Exception $e) {
            return null;
        }
    }
}
