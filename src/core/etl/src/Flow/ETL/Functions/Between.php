<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Kernels\Comparison\ValueOrder;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;
use SensitiveParameter;

use function array_map;
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
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->lowerBoundRef = $lowerBoundRef instanceof ScalarFunction ? $lowerBoundRef : new Literal($lowerBoundRef);
        $this->upperBoundRef = $upperBoundRef instanceof ScalarFunction ? $upperBoundRef : new Literal($upperBoundRef);
        $this->boundary = $boundary instanceof ScalarFunction ? $boundary : new Literal($boundary);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['lower_bound_ref'] ?? null, ScalarFunction::class),
            $rebuild->of($data['upper_bound_ref'] ?? null, ScalarFunction::class),
            $rebuild->of($data['boundary'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'lower_bound_ref' => $this->lowerBoundRef,
            'upper_bound_ref' => $this->upperBoundRef,
            'boundary' => $this->boundary,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $boundaries = (new Parameter($this->boundary))->asEnums($rows, $backend, Boundary::class);
        $columns = array_map(static fn(ScalarFunction $function): Column => $function->eval($rows, $backend), [
            $this->value,
            $this->lowerBoundRef,
            $this->upperBoundRef,
        ]);
        $comparable = new ComparableValues();
        $sameType =
            type_bare($columns[0]->type())::class === type_bare($columns[1]->type())::class
            && type_bare($columns[0]->type())::class === type_bare($columns[2]->type())::class;
        [$values, $lowerBounds, $upperBounds] = array_map(static fn(Column $column): array => $sameType
            ? $comparable->ordering($column)
            : $column->values(), $columns);
        $order = new ValueOrder(...array_map(static fn(Column $column): Type => $column->type(), $columns));
        $results = [];
        $i = 0;

        try {
            foreach ($boundaries as $i => $boundary) {
                if (!$boundary instanceof Boundary) {
                    throw new InvalidArgumentException('Between function requires valid boundary');
                }

                $results[] = $boundary->compare($values[$i], $lowerBounds[$i], $upperBounds[$i], $order);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
