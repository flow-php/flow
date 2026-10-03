<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function get_debug_type;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $haystackParameter = new Parameter($this->haystack);
        $haystackColumn = $haystackParameter->column($rows, $context);
        $needleColumn = (new Parameter($this->needle))->column($rows, $context);
        $haystackType = type_bare($haystackColumn->type());
        $comparable = new ComparableValues();

        // a list of the needle's type compares by the physical form of its elements: list physicals are the
        // element physicals
        if (
            $haystackType instanceof ListType
            && type_bare($haystackType->element())::class === type_bare($needleColumn->type())::class
            && $comparable->equalByPhysical($needleColumn->type())
        ) {
            /** @var list<?array<array-key, mixed>> $haystacks */
            $haystacks = $comparable->equalities($haystackColumn->type(), $haystackColumn->physicals());
            $needles = $comparable->equality($needleColumn);
            $physical = null;
        } else {
            $haystacks = $haystackParameter->arraysOf($haystackColumn);
            $needles = $needleColumn->values();
            // a candidate of the needle's kind compares by its physical form, as the list path above does
            $physical = $comparable->equalByPhysical($needleColumn->type())
                ? (new PhysicalFor())->type(type_bare($needleColumn->type()))
                : null;
        }

        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($needles as $i => $needle) {
                $haystack = $haystacks[$i];

                if ($haystack === null || $needle === null) {
                    $results[] = null;

                    continue;
                }

                // Bind has already proved the pair comparable in returns().
                // @mago-ignore analysis:mixed-assignment
                foreach ($haystack as $candidate) {
                    if ($candidate === null) {
                        continue;
                    }

                    // @mago-expect analysis:mixed-operand(2)
                    if (match (true) {
                        is_int($needle) || is_float($needle) || is_int($candidate) || is_float($candidate) => $needle
                            == $candidate,
                        $needle instanceof DateTimeInterface && $candidate instanceof DateTimeInterface => $needle
                            == $candidate,
                        $physical !== null && get_debug_type($needle) === get_debug_type($candidate)
                            => $comparable->equalities($needleColumn->type(), [$physical->toPhysical(
                            $needle,
                        )]) === $comparable->equalities($needleColumn->type(), [$physical->toPhysical($candidate)]),
                        default => $needle === $candidate,
                    }) {
                        $results[] = true;

                        continue 2;
                    }
                }

                // A match beats a NULL element; no match with a NULL element is unknowable
                $results[] = in_array(null, $haystack, true) ? null : false;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
