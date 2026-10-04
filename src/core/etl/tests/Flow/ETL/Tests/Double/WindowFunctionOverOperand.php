<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Window;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;

/**
 * A window function that evaluates a function operand - the shape a custom window function can take.
 */
final class WindowFunctionOverOperand implements WindowFunction
{
    use ResolvesFromChildren;

    public function __construct(
        public readonly ScalarFunction $operand,
        public readonly ?Window $window = null,
    ) {}

    public function apply(WindowContext $window): mixed
    {
        return $this->operand->eval($window->frame(), $window->flowContext())->value(0);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->operand];
    }

    public function over(Window $window): static
    {
        return new self($this->operand, $window);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->operand->returns());
    }

    public function toString(): string
    {
        return 'operand()';
    }

    public function window(): Window
    {
        return $this->window ?? throw new RuntimeException('operand() requires an OVER clause.');
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->window);
    }
}
