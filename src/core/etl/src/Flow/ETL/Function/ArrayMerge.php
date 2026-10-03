<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_merge;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;

/**
 * Scalar function that takes two other functions, checks if both of them are arrays and merges them.
 */
final class ArrayMerge implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    /**
     * @param array<array-key, mixed>|ScalarFunction $left
     * @param array<array-key, mixed>|ScalarFunction $right
     */
    public function __construct(ScalarFunction|array $left, ScalarFunction|array $right)
    {
        $this->left = $left instanceof ScalarFunction ? $left : lit($left);
        $this->right = $right instanceof ScalarFunction ? $right : lit($right);
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
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $left = type_bare($this->left->returns());
        $right = type_bare($this->right->returns());

        if ($left instanceof ListType && $right instanceof ListType) {
            return type_list(
                (new PromotingUnifier())->unify(
                    $left->element(),
                    $right->element(),
                ) ?? throw InvalidTypeException::noCommonType($left->element(), $right->element()),
            );
        }

        return type_array();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $lefts = (new Parameter($this->left))->asArrays($rows, $context);
        $rights = (new Parameter($this->right))->asArrays($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($lefts as $i => $left) {
                $right = $rights[$i];

                if ($left === null || $right === null) {
                    throw new InvalidArgumentException('ArrayMerge function requires two non-null arrays');
                }

                $results[] = array_merge($left, $right);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
