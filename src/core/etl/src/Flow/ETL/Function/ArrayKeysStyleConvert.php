<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\StyleConverter\ArrayKeyConverter;
use Flow\ETL\Row;
use Flow\ETL\String\StringStyles;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_structure;

final class ArrayKeysStyleConvert implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly StringStyles $style,
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
        return new self($children[0], $this->style);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->ref->returns());

        if (!$array instanceof StructureType) {
            throw SchemaNotDerivableException::function(
                'array_keys_style_convert',
                'the array operand declares "' . $array->toString() . '", which is not a structure',
            );
        }

        $elements = [];

        foreach ($array->elements() as $name => $element) {
            $elements[$this->style->convert((string) $name)] = $element;
        }

        $optional = [];

        foreach ($array->optionalElements() as $name => $element) {
            $optional[$this->style->convert((string) $name)] = $element;
        }

        return type_structure($elements, $optional, $array->allowsExtra());
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);

        if ($array === null) {
            throw new InvalidArgumentException('ArrayKeysStyleConvert function requires non-null array');
        }

        $converter = new ArrayKeyConverter(fn(string $key): string => $this->style->convert($key));

        return $converter->convert($array);
    }
}
