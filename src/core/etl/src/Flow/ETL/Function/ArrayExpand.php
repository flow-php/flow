<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_keys;
use function array_map;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class ArrayExpand implements ScalarFunction, ExpandResults
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ArrayExpand\ArrayExpand $expand,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->expand);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->ref->returns());

        if (!$array instanceof ListType && !$array instanceof MapType && !$array instanceof StructureType) {
            throw SchemaNotDerivableException::function(
                'array_expand',
                'the array operand declares "' . $array->toString() . '", which has no element type',
            );
        }

        // The unification can refuse, so KEYS must not ask for the value type it does not declare.
        $value = fn(): Type => match (true) {
            $array instanceof ListType => $array->element(),
            $array instanceof MapType => $array->value(),
            default => StructureValues::type('array_expand', $array),
        };

        $key = match (true) {
            $array instanceof ListType => type_integer(),
            $array instanceof MapType => $array->key(),
            default => type_string(),
        };

        return match ($this->expand) {
            ArrayExpand\ArrayExpand::VALUES => $value(),
            ArrayExpand\ArrayExpand::KEYS => $key,
            ArrayExpand\ArrayExpand::BOTH => type_map($key, $value()),
        };
    }

    /**
     * @return array<mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);

        if ($array === null) {
            throw new InvalidArgumentException('ArrayExpand requires non-null array');
        }

        if ($this->expand === ArrayExpand\ArrayExpand::KEYS) {
            return array_keys($array);
        }

        if ($this->expand === ArrayExpand\ArrayExpand::BOTH) {
            return array_map(static fn($key, $value) => [$key => $value], array_keys($array), $array);
        }

        return $array;
    }
}
