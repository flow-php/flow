<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;

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
        private readonly OnEachElementSchema $elementSchema = new OnEachElementSchema(),
    ) {
        (new ExpandingFunctions())->refuse($function, 'onEach');

        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
    }

    /**
     * The lambda body evaluates against a synthesised one-column row, so ref('element') inside it
     * indexes a different input - the outer resolver must not touch it.
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
        return new self($children[0], $this->function, $this->preserveKeys, $this->elementSchema);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $arrayType = type_bare($this->array->returns());
        $inner = $this->elementSchema->of($arrayType);

        $body = (new ReferenceResolver())->resolve($this->function, $inner);

        (new ReferenceResolver())->assertResolved($body, $inner);

        $bodyType = $body->returns();

        if ($this->preserveKeys && $arrayType instanceof StructureType) {
            return new StructureType(array_map(
                static fn(StructureElement $element): StructureElement => structure_element(
                    $element->name,
                    $bodyType,
                    $element->optional,
                ),
                $arrayType->elements(),
            ));
        }

        if ($this->preserveKeys && $arrayType instanceof MapType) {
            return type_map($arrayType->key(), $bodyType);
        }

        return type_list($bodyType);
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->array))->asArray($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('OnEach requires non-null array');
        }

        $output = [];

        $hydrator = $context->hydrator();
        // hoisted: array_to_row() runs per element, and the schema is the same for every one of them
        $elementSchema = $this->elementSchema->of(type_bare($this->array->returns()));

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            $result = (new Parameter($this->function))->eval(
                array_to_row(['element' => $item], $elementSchema, $hydrator),
                $context,
            );

            if ($this->preserveKeys) {
                $output[$key] = $result;
            } else {
                $output[] = $result;
            }
        }

        return $output;
    }
}
