<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;

final class Equals implements ComparisonFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(mixed $left, mixed $right)
    {
        $this->left = $left instanceof ScalarFunction ? $left : lit($left);
        $this->right = $right instanceof ScalarFunction ? $right : lit($right);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function operands(): array
    {
        return [$this->left, $this->right];
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
        return new self($children[0], $children[1]);
    }

    /**
     * @param list<ScalarFunction> $operands
     */
    public function withOperands(array $operands): static
    {
        return new self($operands[0], $operands[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        (new ValueComparator())->assertComparableTypes($this->left->returns(), $this->right->returns(), '==');

        return (new Nullability())->any(type_boolean(), $this->left->returns(), $this->right->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $leftColumn = (new Parameter($this->left))->column($rows, $context);
        $rightColumn = (new Parameter($this->right))->column($rows, $context);
        $comparable = new ComparableValues();
        $sameType = type_bare($leftColumn->type())::class === type_bare($rightColumn->type())::class;
        [$lefts, $rights] = $sameType
            ? [$comparable->equality($leftColumn), $comparable->equality($rightColumn)]
            : [$leftColumn->values(), $rightColumn->values()];
        $results = [];
        $i = 0;

        // same-typed physicals: every arm of the dispatch below is === on them
        if ($sameType && $comparable->equalByPhysical($leftColumn->type())) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                $results[] = $left === null || $rights[$i] === null ? null : $left === $rights[$i];
            }

            return (new ResultColumn($context->backend()))->of($this, $results);
        }

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];

                if ($left === null || $right === null) {
                    $results[] = null;

                    continue;
                }

                // The dispatch picks a comparison strategy from the values, never a column type - bind has
                // already proved the pair comparable in returns().
                // @mago-expect analysis:mixed-operand(2)
                $results[] = match (true) {
                    is_int($left) || is_float($left) || is_int($right) || is_float($right) => $left == $right,
                    $left instanceof DateTimeInterface && $right instanceof DateTimeInterface => $left == $right,
                    $left instanceof DateInterval && $right instanceof DateInterval => (new DateTimeImmutable(
                        '@0',
                    ))->add($left) == (new DateTimeImmutable('@0'))->add($right),
                    default => $left === $right,
                };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
