<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Concat implements Expression
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
        $values = \array_map(function (Expression $ref) use ($row) : mixed {
            $ref = new Expressions(new Cast($ref, 'string'));

            return $ref->eval($row);
        }, $this->refs);

        foreach ($values as $value) {
            if (!\is_string($value)) {
                return null;
            }
        }

        /** @var array<string> $values */
        return \implode('', $values);
    }
}
