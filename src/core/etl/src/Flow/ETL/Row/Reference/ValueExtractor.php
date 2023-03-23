<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression\Literal;

final class ValueExtractor
{
    public function value(Row $row, EntryReference|Literal $ref, mixed $default = null) : mixed
    {
        if ($ref instanceof Literal) {
            return $ref->value();
        }

        $expression = $ref->expressions();

        $literal = $expression->literal();

        if ($literal instanceof Literal) {
            return $literal->value();
        }

        if ($row->has($ref)) {
            return $row->get($ref)->value();
        }

        return $default;
    }
}
