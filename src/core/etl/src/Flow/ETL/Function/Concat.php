<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;

final class Concat extends ScalarFunctionChain
{
    /**
     * @var array<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        $values = \array_map(function (ScalarFunction $ref) use ($row) : mixed {
            return Caster::default()->to(type_string(true))->value($ref->eval($row));
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
