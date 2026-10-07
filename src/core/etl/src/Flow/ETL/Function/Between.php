<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;

final class Between implements ComparisonFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $lowerBoundRef;
    private readonly ScalarFunction $upperBoundRef;
    private readonly ScalarFunction $boundary;

    public function __construct(
        mixed $value,
        mixed $lowerBoundRef,
        mixed $upperBoundRef,
        ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->lowerBoundRef = $lowerBoundRef instanceof ScalarFunction ? $lowerBoundRef : lit($lowerBoundRef);
        $this->upperBoundRef = $upperBoundRef instanceof ScalarFunction ? $upperBoundRef : lit($upperBoundRef);
        $this->boundary = $boundary instanceof ScalarFunction ? $boundary : lit($boundary);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function operands(): array
    {
        return [$this->value, $this->lowerBoundRef, $this->upperBoundRef];
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->lowerBoundRef, $this->upperBoundRef, $this->boundary];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @param list<ScalarFunction> $operands
     */
    public function withOperands(array $operands): static
    {
        return new self($operands[0], $operands[1], $operands[2], $this->boundary);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        (new ValueComparator())->assertAllTypesComparable([
            $this->value->returns(),
            $this->lowerBoundRef->returns(),
            $this->upperBoundRef->returns(),
        ], Operator::GREATER_THAN_EQUAL);

        return (new Nullability())->any(
            type_boolean(),
            $this->value->returns(),
            $this->lowerBoundRef->returns(),
            $this->upperBoundRef->returns(),
            $this->boundary->returns(),
        );
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $boundaries = (new Parameter($this->boundary))->asEnums($rows, $context, Boundary::class);
        $columns = array_map(
            static fn(ScalarFunction $function): Column => (new Parameter($function))->column($rows, $context),
            [$this->value, $this->lowerBoundRef, $this->upperBoundRef],
        );
        $comparable = new ComparableValues();
        $sameType =
            type_bare($columns[0]->type())::class === type_bare($columns[1]->type())::class
            && type_bare($columns[0]->type())::class === type_bare($columns[2]->type())::class;
        [$values, $lowerBounds, $upperBounds] = array_map(static fn(Column $column): array => $sameType
            ? $comparable->ordering($column)
            : $column->values(), $columns);
        $results = [];
        $i = 0;

        try {
            foreach ($boundaries as $i => $boundary) {
                if (!$boundary instanceof Boundary) {
                    throw new InvalidArgumentException('Between function requires valid boundary');
                }

                $results[] = $boundary->compare($values[$i], $lowerBounds[$i], $upperBounds[$i]);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
