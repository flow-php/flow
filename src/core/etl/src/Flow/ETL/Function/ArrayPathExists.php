<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final class ArrayPathExists implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param ScalarFunction|string $path
     */
    private readonly ScalarFunction $array;
    private readonly ScalarFunction $path;

    public function __construct(ScalarFunction|array $array, ScalarFunction|string $path)
    {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
        $this->path = $path instanceof ScalarFunction ? $path : lit($path);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array, $this->path];
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
        return (new Nullability())->any(type_boolean(), $this->array->returns(), $this->path->returns());
    }

    public function eval(Row $row, FlowContext $context): bool
    {
        try {
            $array = (new Parameter($this->array))->asArray($row, $context);
            $path = (new Parameter($this->path))->asString($row, $context);

            if ($array === null || $path === null) {
                throw new InvalidArgumentException('ArrayPathExists function requires non-null array and path');
            }

            return array_dot_exists($array, $path);
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('ArrayPathExists error: ' . $e->getMessage());
        }
    }
}
