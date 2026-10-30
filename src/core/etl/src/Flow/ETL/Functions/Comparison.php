<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\Comparison\ValueEquality;
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

use function constant;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;
use function sprintf;

final class Comparison implements ComparisonFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(
        mixed $left,
        public readonly Operator $operator,
        mixed $right,
    ) {
        if ($operator === Operator::DIFFERENT || $operator === Operator::SPACE_SHIP) {
            throw new InvalidArgumentException(sprintf(
                'Comparison does not support "%s", use one of: ==, !=, ===, !==, >, >=, <, <=',
                $operator->value,
            ));
        }

        $this->left = $left instanceof ScalarFunction ? $left : new Literal($left);
        $this->right = $right instanceof ScalarFunction ? $right : new Literal($right);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['left'] ?? null, ScalarFunction::class),
            type_enum(Operator::class)->assert(constant(
                Operator::class . '::' . type_string()->assert($data['operator'] ?? null),
            )),
            $rebuild->of($data['right'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'left' => $this->left,
            'operator' => $this->operator->name,
            'right' => $this->right,
        ]);
    }

    /**
     * Identity compares without coercion: a temporal string never becomes identical to a date.
     *
     * @return list<ScalarFunction>
     */
    public function operands(): array
    {
        return $this->identity() ? [] : [$this->left, $this->right];
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->left, $this->right];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->operator, $children[1]);
    }

    /**
     * @param list<ScalarFunction> $operands
     */
    public function withOperands(array $operands): static
    {
        return new self($operands[0], $this->operator, $operands[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        (new ValueComparator())->assertComparableTypes(
            $this->left->returns(),
            $this->right->returns(),
            $this->operator,
        );

        return $this->identity()
            ? type_boolean()
            : (new Nullability())->any(type_boolean(), $this->left->returns(), $this->right->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $left = $this->left->eval($rows, $backend);
        $right = $this->right->eval($rows, $backend);
        $order = new ValueOrder($left->type(), $right->type());
        $results = match ($this->operator) {
            Operator::EQUAL, Operator::NOT_EQUAL => (new ValueEquality())->equal(
                $left,
                $right,
                $this->left instanceof Reference ? $this->left->to() : 'left',
                $this->right instanceof Reference ? $this->right->to() : 'right',
            ),
            Operator::IDENTICAL, Operator::NOT_IDENTICAL => (new ValueEquality())->identical($left, $right),
            Operator::GREATER_THAN => $order->lessThan(...$this->ordering($right, $left)),
            Operator::GREATER_THAN_EQUAL => $order->lessThanOrEqual(...$this->ordering($right, $left)),
            Operator::LESS_THAN => $order->lessThan(...$this->ordering($left, $right)),
            Operator::LESS_THAN_EQUAL => $order->lessThanOrEqual(...$this->ordering($left, $right)),
            Operator::DIFFERENT, Operator::SPACE_SHIP => throw new InvalidArgumentException(
                'Comparison was built with an unsupported operator',
            ),
        };

        return (new ResultColumn($backend))->booleans(
            $this->operator === Operator::NOT_EQUAL || $this->operator === Operator::NOT_IDENTICAL
                ? $this->negated($results)
                : $results,
        );
    }

    private function identity(): bool
    {
        return $this->operator === Operator::IDENTICAL || $this->operator === Operator::NOT_IDENTICAL;
    }

    /**
     * @param list<?bool> $results
     *
     * @return list<?bool>
     */
    private function negated(array $results): array
    {
        $negated = [];

        foreach ($results as $result) {
            $negated[] = $result === null ? null : !$result;
        }

        return $negated;
    }

    /**
     * Columns of one type order by their comparable form, mixed types by their values.
     *
     * @return array{list<mixed>, list<mixed>}
     */
    private function ordering(Column $first, Column $second): array
    {
        $comparable = new ComparableValues();

        return (
            type_bare($first->type())::class === type_bare($second->type())::class
                ? [$comparable->ordering($first), $comparable->ordering($second)]
                : [$first->values(), $second->values()]
        );
    }
}
