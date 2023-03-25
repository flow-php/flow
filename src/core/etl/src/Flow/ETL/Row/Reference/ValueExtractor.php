<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\Expressions;
use Flow\ETL\Row\Reference\Expression\Literal;

final class ValueExtractor
{
    public function value(Row $row, Expression $ref, mixed $default = null) : mixed
    {
        if ($ref instanceof Literal) {
            return $ref->value();
        }

        $expressions = $ref instanceof Row\EntryReference
            ? $ref->expressions()
            : null;

        $literal = ($expressions instanceof Expressions) ? $expressions->literal() : null;

        if ($literal instanceof Literal) {
            return $literal->value();
        }

        if ($ref instanceof Row\EntryReference && $row->has($ref)) {
            return $row->get($ref)->value();
        }

        return $default;
    }
}
