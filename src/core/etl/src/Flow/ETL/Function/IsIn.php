<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function in_array;

final class IsIn implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $haystack;
    private readonly ScalarFunction $needle;

    /**
     * @param array<array-key, mixed>|ScalarFunction $haystack
     */
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
        $haystack = $this->haystack->returns();
        $needle = $this->needle->returns();
        $nullability = new Nullability();
        $bare = $nullability->bare($haystack);

        // Only a list and a map declare one element type to compare the needle against. A structure,
        // a json or a bare array<mixed> declares none, and Parameter::asArray() iterates all of them,
        // so asserting there would refuse haystacks eval() handles.
        $element = match (true) {
            $bare instanceof ListType => $nullability->bare($bare->element()),
            $bare instanceof MapType => $nullability->bare($bare->value()),
            default => null,
        };

        if ($element !== null) {
            (new ValueComparator())->assertComparableTypes($nullability->bare($needle), $element, '==');
        }

        return $nullability->any(type_boolean(), $haystack, $needle);
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $haystack = (new Parameter($this->haystack))->asArray($row, $context);
        $needle = (new Parameter($this->needle))->eval($row, $context);

        if ($haystack === null || $needle === null) {
            return null;
        }

        // Equals::eval()'s dispatch minus its DateInterval arm, which IsIn never had: two equal
        // DateIntervals still compare equal under equals() and NOT equal under isIn(). Bind has
        // already proved the pair comparable in returns().
        // @mago-ignore analysis:mixed-assignment
        foreach ($haystack as $candidate) {
            if ($candidate === null) {
                continue;
            }

            // @mago-ignore analysis:mixed-operand
            if (match (true) {
                is_int($needle) || is_float($needle) || is_int($candidate) || is_float($candidate) => $needle
                    == $candidate,
                $needle instanceof DateTimeInterface && $candidate instanceof DateTimeInterface => $needle
                    == $candidate,
                default => $needle === $candidate,
            }) {
                return true;
            }
        }

        // A match beats a NULL element; no match with a NULL element is unknowable
        return in_array(null, $haystack, true) ? null : false;
    }
}
