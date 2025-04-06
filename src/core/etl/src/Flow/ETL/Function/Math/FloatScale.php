<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Math;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Function\{Parameter, ScalarFunction};
use Flow\ETL\Row;

final readonly class FloatScale
{
    private ScalarFunction $function;

    private ScalarFunction $scale;

    public function __construct(mixed $function, ScalarFunction|int|null $scale = null)
    {
        $this->scale = $scale instanceof ScalarFunction ? $scale : lit($scale);
        $this->function = $function instanceof ScalarFunction ? $function : lit($function);
    }

    public function scale(Row $row) : int
    {
        $scale = (new Parameter($this->scale))->asInt($row, 0);

        if ($scale > 0) {
            return $scale;
        }

        $entry = (new Parameter($this->function))->asEntry($row);

        if ($entry instanceof Row\Entry\FloatEntry) {
            $scale = $entry->precision;
        } else {
            $value = (new Parameter($this->function))->asNumber($row);

            if (\is_float($value)) {
                $scale = 6;
            }
        }

        return $scale;
    }
}
