<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Symfony\Component\String\AbstractString;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Chunk implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $size;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $size)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->size = $size instanceof ScalarFunction ? $size : lit($size);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->size];
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
        return type_list(type_string());
    }

    /**
     * @return array<int, string>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $size = (new Parameter($this->size))->asInt($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('Chunk function requires non-null value');
        }

        if ($size === null || $size <= 0) {
            throw new InvalidArgumentException('Chunk function requires non-null, positive size');
        }

        $chunks = s($value)->chunk($size);

        return array_map(
            static fn(AbstractString $chunk): string => $chunk->toString(),
            iterator_to_array($chunks, false),
        );
    }
}
