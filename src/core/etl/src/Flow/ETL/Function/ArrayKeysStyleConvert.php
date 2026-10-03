<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\StyleConverter\ArrayKeyConverter;
use Flow\ETL\Rows;
use Flow\ETL\String\StringStyles;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;

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

        return $this->converted($array);
    }

    /**
     * $type with every structure field renamed the way eval() renames keys - at every depth, as ArrayKeyConverter
     * recurses into nested arrays.
     *
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function converted(Type $type): Type
    {
        if ($type instanceof OptionalType) {
            return type_optional($this->converted($type->base()));
        }

        if ($type instanceof ListType) {
            return type_list($this->converted($type->element()));
        }

        if ($type instanceof MapType) {
            return type_map($type->key(), $this->converted($type->value()));
        }

        if (!$type instanceof StructureType) {
            return $type;
        }

        $elements = [];
        $sources = [];

        foreach ($type->elements() as $element) {
            $converted = $this->style->convert((string) $element->name);

            if (array_key_exists($converted, $sources)) {
                throw SchemaNotDerivableException::function(
                    'array_keys_style_convert',
                    'fields "'
                    . $sources[$converted]
                    . '" and "'
                    . $element->name
                    . '" of "'
                    . $type->toString()
                    . '" both convert to "'
                    . $converted
                    . '"',
                );
            }

            $sources[$converted] = $element->name;
            $elements[] = structure_element($converted, $this->converted($element->type), $element->optional);
        }

        return new StructureType($elements);
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $arrays = (new Parameter($this->ref))->asArrays($rows, $context);
        $converter = new ArrayKeyConverter(fn(string $key): string => $this->style->convert($key));
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                if ($array === null) {
                    throw new InvalidArgumentException('ArrayKeysStyleConvert function requires non-null array');
                }

                $results[] = $converter->convert($array);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
