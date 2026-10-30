<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\Window;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_optional;

/**
 * A window function that evaluates a function operand - the shape a custom window function can take.
 */
final class WindowFunctionOverOperand implements WindowFunction
{
    public function __construct(
        public readonly ScalarFunction $operand,
        public readonly ?Window $window = null,
    ) {}

    /**
     * @return list<mixed>
     */
    public function values(Rows $partition, Backend $backend): array
    {
        return $this->operand->eval($partition, $backend)->values();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->operand];
    }

    public function deterministic(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->deterministic()) {
                return false;
            }
        }

        return true;
    }

    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

        return true;
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
