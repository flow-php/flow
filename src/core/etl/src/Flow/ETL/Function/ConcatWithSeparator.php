<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
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

        $values = \array_map(fn (ScalarFunction|string $string) : mixed => \is_string($string) ? $string : Caster::default()->to(type_string(true))->value($string->eval($row)), $this->refs);

        $concatValues = [];

        foreach ($values as $value) {
            if (\is_string($value)) {
                $concatValues[] = $value;
            }
        }

        if (\count($concatValues) === 0) {
            return '';
        }

        /** @var array<string> $values */
        return \implode($separator, $concatValues);
    }
}
