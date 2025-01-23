<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\{Caster, Type};
use Flow\ETL\Row;

final class Concat extends ScalarFunctionChain implements TypedScalarFunction
{
    /**
     * @var array<ScalarFunction|string>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction|string ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = \is_string($value) ? $value : Caster::default()->to(type_string(true))->value($value->eval($row));

            if (\is_string($value)) {
                $concatValues[] = $value;
            }
        }

        return \implode('', $concatValues);
    }

    public function returns() : Type
    {
        return type_string();
    }
}
