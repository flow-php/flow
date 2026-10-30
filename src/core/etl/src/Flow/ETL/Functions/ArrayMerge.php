<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Unifier\PromotingUnifier;
use SensitiveParameter;

use function array_merge;
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
        $this->left = $left instanceof ScalarFunction ? $left : new Literal($left);
        $this->right = $right instanceof ScalarFunction ? $right : new Literal($right);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['left'] ?? null, ScalarFunction::class),
            $rebuild->of($data['right'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'left' => $this->left,
            'right' => $this->right,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lefts = (new Parameter($this->left))->asArrays($rows, $backend);
        $rights = (new Parameter($this->right))->asArrays($rows, $backend);
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
