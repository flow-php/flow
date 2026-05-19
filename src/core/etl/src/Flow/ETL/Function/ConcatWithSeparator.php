<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function array_merge;
use function Flow\ETL\DSL\is_type;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;

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

    public function eval(Row $row, FlowContext $context): mixed
    {
        $separator = (new Parameter($this->separator))->asString($row, $context);

        if (!is_string($separator)) {
            $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('ConcatWithSeparator function requires non-null separator'),
                );

            return '';
        }

        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = (new Parameter($value))->eval($row, $context);

            if (is_type(type_list(type_string()), $value)) {
                /** @var list<string> $value */
                $concatValues = array_merge($concatValues, $value);
            } else {
                $value = is_string($value) ? $value : type_optional(type_string())->cast($value);

                if (is_string($value)) {
                    $concatValues[] = $value;
                }
            }
        }

        return implode($separator, $concatValues);
    }
}
