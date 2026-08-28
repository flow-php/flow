<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function in_array;

final class IsIn implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @param array<array-key, mixed>|ScalarFunction $haystack
     * @param mixed $needle
     */
    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $needle;

    public function __construct(ScalarFunction|array $haystack, mixed $needle)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : lit($haystack);
        $this->needle = $needle instanceof ScalarFunction ? $needle : lit($needle);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->needle];
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
        return (new Nullability())->any(type_boolean(), $this->haystack->returns(), $this->needle->returns());
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $haystack = (new Parameter($this->haystack))->asArray($row, $context);

        if ($haystack === null) {
            throw new InvalidArgumentException('IsIn function requires non-null array');
        }

        return in_array((new Parameter($this->needle))->eval($row, $context), $haystack, true);
    }
}
