<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final class Equals implements ScalarFunction
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
        (new ValueComparator())->assertComparableTypes($this->left->returns(), $this->right->returns(), '==');

        return (new Nullability())->any(type_boolean(), $this->left->returns(), $this->right->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $left = (new Parameter($this->left))->eval($row, $context);
        $right = (new Parameter($this->right))->eval($row, $context);

        if ($left === null || $right === null) {
            return null;
        }

        // The dispatch picks a comparison strategy from the values, never a column type - bind has
        // already proved the pair comparable in returns().
        return match (true) {
            is_int($left) || is_float($left) || is_int($right) || is_float($right) => $left == $right,
            $left instanceof DateTimeInterface && $right instanceof DateTimeInterface => $left == $right,
            $left instanceof DateInterval && $right instanceof DateInterval => (new DateTimeImmutable('@0'))->add(
                $left,
            ) == (new DateTimeImmutable('@0'))->add($right),
            default => $left === $right,
        };
    }
}
