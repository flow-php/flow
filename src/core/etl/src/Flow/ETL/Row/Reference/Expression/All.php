<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class All implements Expression
{
    /**
     * @var array<Expression>
     */
    private readonly array $refs;

    public function __construct(
        Expression ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->refs as $ref) {
            if (!$ref->eval($row)) {
                return false;
            }
        }

        return true;
    }
}
