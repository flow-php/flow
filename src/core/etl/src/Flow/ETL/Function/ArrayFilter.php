<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;

use function array_filter;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;

final class ArrayFilter implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;
    private readonly ScalarFunction $value;

    /**
     * @param ScalarFunction|array<array-key, mixed> $array
     */
    public function __construct(ScalarFunction|array $array, mixed $value = null)
    {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array, $this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->array->returns());

        if ($array instanceof ListType) {
            return type_map(type_integer(), $array->element());
        }

        return $array;
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if (null === $array) {
            throw new InvalidArgumentException('ArrayFilter function requires non-null array');
        }

        $value = (new Parameter($this->value))->eval($row, $context);

        return array_filter($array, static fn($item) => $item !== $value);
    }
}
