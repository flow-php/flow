<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function array_map;
use function array_merge;
use function array_slice;
use function array_values;
use function Flow\ETL\DSL\is_type;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;

final class ConcatWithSeparator implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $refs;

    private readonly ScalarFunction $separator;

    public function __construct(ScalarFunction|string $separator, ScalarFunction|string ...$refs)
    {
        $this->separator = $separator instanceof ScalarFunction ? $separator : lit($separator);
        $this->refs = array_values(array_map(static fn(ScalarFunction|string $ref): ScalarFunction => $ref
            instanceof ScalarFunction
                ? $ref
                : lit($ref), $refs));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->separator, ...$this->refs];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], ...array_slice($children, 1));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): string
    {
        $separator = (new Parameter($this->separator))->asString($row, $context);

        if (!is_string($separator)) {
            throw new InvalidArgumentException('ConcatWithSeparator function requires non-null separator');
        }

        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = (new Parameter($value))->eval($row, $context);

            if (is_type(type_list(type_string()), $value)) {
                /** @var list<string> $value */
                $concatValues = array_merge($concatValues, $value);
            } elseif ($value !== null) {
                $concatValues[] = type_string()->cast($value);
            }
        }

        return implode($separator, $concatValues);
    }
}
