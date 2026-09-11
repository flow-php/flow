<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final class NotSame implements ScalarFunction
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
        (new ValueComparator())->assertComparableTypes($this->left->returns(), $this->right->returns(), '!==');

        return type_boolean();
    }

    public function eval(Row $row, FlowContext $context): bool
    {
        // PHP identity, not SQL equality: null !== null is a defined, useful answer
        return (
            (new Parameter($this->left))->eval($row, $context) !== (new Parameter($this->right))->eval($row, $context)
        );
    }
}
