<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;

final class OnEach implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(
        ScalarFunction|array $array,
        private readonly ScalarFunction $function,
        private readonly bool $preserveKeys = true,
    ) {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
    }

    /**
     * The lambda body evaluates against a synthesised one-column row, so ref('element') inside it
     * indexes a different input - the outer resolver must not touch it (DuckDB's BOUND_LAMBDA rule).
     *
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
        return new self($children[0], $this->function, $this->preserveKeys);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $arrayType = type_bare($this->array->returns());
        $element = match (true) {
            $arrayType instanceof ListType => $arrayType->element(),
            $arrayType instanceof MapType => $arrayType->value(),
            default => throw SchemaNotDerivableException::function(
                'on_each',
                'the array operand declares "' . $arrayType->toString() . '", which has no element type',
            ),
        };

        $inner = schema(definition_from_type('element', $element));

        /** @var ScalarFunction $body resolve() preserves the node's class for a non-leaf root */
        $body = (new ReferenceResolver())->resolve($this->function, $inner);

        (new ReferenceResolver())->assertResolved($body, $inner);

        return type_list($body->returns());
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->array))->asArray($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('OnEach requires non-null array');
        }

        $output = [];

        $hydrator = $context->hydrator();

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            if ($this->preserveKeys) {
                $output[$key] = (new Parameter($this->function))->eval(array_to_row([
                    'element' => $item,
                ], $hydrator), $context);
            } else {
                $output[] = (new Parameter($this->function))->eval(array_to_row([
                    'element' => $item,
                ], $hydrator), $context);
            }
        }

        return $output;
    }
}
