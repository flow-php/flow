<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_keys;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
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

        $this->array = $array instanceof ScalarFunction ? $array : new Literal($array);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['array'] ?? null, ScalarFunction::class),
            $rebuild->of($data['function'] ?? null, ScalarFunction::class),
            type_boolean()->assert($data['preserve_keys'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'array' => $this->array,
            'function' => $this->function,
            'preserve_keys' => $this->preserveKeys,
        ]);
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

            $body = (new ReferenceResolver())->bind($this->function, $schema, 'onEach');

            $this->bound = [$schema, $body, type_optional($this->typeOver($arrayType, $body))];
        }

        return $this->bound;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        [, $body] = $this->bound();

        return $this->typeOver(type_bare($this->array->returns()), $body);
    }

    /**
     * The produced type for an operand of $arrayType - one read of the operand's returns() serves eval() and bind.
     *
     * @param Type<mixed> $arrayType
     *
     * @return Type<mixed>
     */
    public function typeOver(Type $arrayType, ScalarFunction $body): Type
    {
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $backend);
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
                (new RowsBuilder($elementSchema, $backend))
                    ->appendRecords($elements)
                    ->finish(),
                $backend,
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

        return (new ResultColumn($backend))->typed($type, $output);
    }
}
