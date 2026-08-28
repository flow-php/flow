<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Throwable;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function Symfony\Component\String\s;

final class StringMatch implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $pattern;

    public function __construct(ScalarFunction|string $haystack, ScalarFunction|string $pattern)
    {
        $this->haystack = $haystack instanceof ScalarFunction ? $haystack : lit($haystack);
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : lit($pattern);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->haystack, $this->pattern];
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
        return type_optional(type_array());
    }

    /**
     * @return null|array<int|string, string>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $haystack = (new Parameter($this->haystack))->asString($row, $context);
        $pattern = (new Parameter($this->pattern))->asString($row, $context);

        if ($haystack === null) {
            throw new InvalidArgumentException('StringMatch function requires non-null haystack');
        }

        if ($pattern === null) {
            throw new InvalidArgumentException('StringMatch function requires non-null pattern');
        }

        try {
            /** @var array<int|string, string> $result */
            $result = s($haystack)->match($pattern);

            return count($result) > 0 ? $result : null;
        } catch (Throwable $e) {
            throw new InvalidArgumentException('StringMatch error: ' . $e->getMessage());
        }
    }
}
