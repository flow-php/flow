<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;

use function is_bool;
use function is_float;
use function is_int;
use function str_replace;
use function substr;

final readonly class ColumnDefaultFormatter
{
    public function format(bool|float|int|string|Expression|null $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof Expression) {
            return substr(SelectBuilder::create()->select($value)->toSql(), 7);
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        return "'" . str_replace("'", "''", $value) . "'";
    }
}
