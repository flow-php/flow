<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{is_type, type_list, type_string};
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;

final class ConcatWithSeparator extends ScalarFunctionChain
{
    /**
     * @var array<ScalarFunction|string>
     */
    private readonly array $refs;

    public function __construct(
        private readonly ScalarFunction|string $separator,
        ScalarFunction|string ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        $separator = (new Parameter($this->separator))->asString($row);

        if (!\is_string($separator)) {
            return '';
        }

        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = (new Parameter($value))->eval($row);

            if (is_type(type_list(type_string()), $value)) {
                /** @var list<string> $value */
                $concatValues = \array_merge($concatValues, $value);
            } else {
                $value = \is_string($value) ? $value : Caster::default()->to(type_string(true))->value($value);

                if (\is_string($value)) {
                    $concatValues[] = $value;
                }
            }
        }

        return \implode($separator, $concatValues);
    }
}
