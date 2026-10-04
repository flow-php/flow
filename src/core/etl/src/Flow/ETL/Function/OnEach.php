<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;

use function array_keys;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;

final class OnEach implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;

    /**
     * @var null|array{Schema, ScalarFunction, Type<mixed>}
     */
    private ?array $bound = null;

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
     * The element schema, the body resolved against it and the result type - derived once per node, not per batch.
     *
     * @return array{Schema, ScalarFunction, Type<mixed>}
     */
    public function bound(): array
    {
        if ($this->bound === null) {
            $arrayType = type_bare($this->array->returns());
            $schema = $this->elementSchema->of($arrayType);

            $this->bound = [
                $schema,
                (new ReferenceResolver())->resolve($this->function, $schema),
                type_optional($this->typeOver($arrayType)),
            ];
        }

        return $this->bound;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->typeOver(type_bare($this->array->returns()));
    }

    /**
     * The produced type for an operand of $arrayType - one read of the operand's returns() serves eval() and bind.
     *
     * @param Type<mixed> $arrayType
     *
     * @return Type<mixed>
     */
    public function typeOver(Type $arrayType): Type
    {
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $context);
        $elements = [];

        /** @var list<int> $rowOf the row of $rows every element comes from */
        $rowOf = [];

        foreach ($arrays as $i => $array) {
            if ($array === null) {
                throw new EvaluationException($i, new InvalidArgumentException('OnEach requires non-null array'));
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($array as $item) {
                $elements[] = ['element' => $item];
                $rowOf[] = $i;
            }
        }

        [$elementSchema, $body, $type] = $this->bound();

        try {
            $results = (new Parameter($body))->values(
                array_to_rows($elements, $elementSchema, $context->backend()),
                $context,
            );
        } catch (EvaluationException $e) {
            throw EvaluationException::at($rowOf[$e->rowIndex], $e);
        }

        $output = [];
        $position = 0;

        foreach ($arrays as $array) {
            $values = [];

            foreach (array_keys($array) as $key) {
                if ($this->preserveKeys) {
                    $values[$key] = $results[$position++];
                } else {
                    $values[] = $results[$position++];
                }
            }

            $output[] = $values;
        }

        return (new ResultColumn($context->backend()))->typed($type, $output);
    }
}
