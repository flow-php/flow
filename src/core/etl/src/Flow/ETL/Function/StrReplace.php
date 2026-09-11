<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function str_replace;

final class StrReplace implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $search;
    private readonly ScalarFunction $replace;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $search
     * @param array<array-key, mixed>|ScalarFunction|string $replace
     */
    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string|array $search,
        ScalarFunction|string|array $replace,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->search = $search instanceof ScalarFunction ? $search : lit($search);
        $this->replace = $replace instanceof ScalarFunction ? $replace : lit($replace);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->search, $this->replace];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $search = (new Parameter($this->search))->as($row, $context, type_string(), type_array());
        $replace = (new Parameter($this->replace))->as($row, $context, type_string(), type_array());

        if ($value === null) {
            throw new InvalidArgumentException('StrReplace function requires non-null value');
        }

        if ($search === null || $replace === null) {
            throw new InvalidArgumentException('StrReplace function requires non-null search and replace');
        }

        $typedSearch = type_union(type_string(), type_list(type_string()))->assert($search);
        $typedReplace = type_union(type_string(), type_list(type_string()))->assert($replace);

        return str_replace($typedSearch, $typedReplace, $value);
    }
}
