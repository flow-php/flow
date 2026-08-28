<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function array_reverse;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;

final class ArrayReverse implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    private readonly ScalarFunction $array;

    public function __construct(
        ScalarFunction|array $array,
        private readonly bool $preserveKeys,
    ) {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->preserveKeys);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_bare($this->array->returns());
    }

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if ($array === null) {
            throw new InvalidArgumentException('ArrayReverse function requires non-null array');
        }

        return array_reverse($array, $this->preserveKeys);
    }
}
