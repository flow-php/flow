<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;

final class ValueExtractor
{
    public function value(Row $row, EntryReference $ref, mixed $default = null) : mixed
    {
        $expression = $ref->expression();

        if ($expression instanceof Row\Reference\Expression\Literal) {
            return $expression->value();
        }

        if ($row->has($ref)) {
            return $row->get($ref)->value();
        }

        return $default;
    }
}
