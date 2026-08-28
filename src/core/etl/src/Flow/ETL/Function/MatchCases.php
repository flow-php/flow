<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\MatchCases\MatchCondition;
use Flow\ETL\Row;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;

use function array_map;
use function array_pop;
use function array_values;
use function count;
use function Flow\ETL\DSL\lit;
use function json_encode;

final class MatchCases implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<MatchCondition>
     */
    private readonly array $cases;

    /**
     * Null means "no default" - never lit(null), so the no-match arm below stays reachable.
     */
    private readonly ?ScalarFunction $default;

    /**
     * @param array<MatchCondition> $cases
     */
    public function __construct(array $cases, mixed $default = null)
    {
        $this->cases = array_values($cases);
        $this->default = $default === null ? null : ($default instanceof ScalarFunction ? $default : lit($default));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->default === null ? $this->cases : [...$this->cases, $this->default];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        $default = count($children) > count($this->cases) ? array_pop($children) : null;

        $cases = [];

        foreach ($children as $case) {
            if (!$case instanceof MatchCondition) {
                throw InvalidLogicException::because(
                    'MatchCases child must be a MatchCondition, got "%s".',
                    $case::class,
                );
            }

            $cases[] = $case;
        }

        return new self($cases, $default);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $branches = array_map(static fn(MatchCondition $case): Type => $case->returns(), $this->cases);

        if ($this->default !== null) {
            $branches[] = $this->default->returns();
        }

        return (
            (new PromotingUnifier())->unifyAll(
                NullabilityRule::ANY,
                ...$branches,
            ) ?? throw InvalidTypeException::noCommonType(...$branches)
        );
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        foreach ($this->cases as $condition) {
            if ($condition->valid($row, $context)) {
                return $condition->eval($row, $context);
            }
        }

        if ($this->default !== null) {
            return (new Parameter($this->default))->eval($row, $context);
        }

        throw new InvalidArgumentException(
            'Not a single case matches row, consider using default parameter, row: '
                . json_encode($row->toArray(), JSON_THROW_ON_ERROR),
        );
    }
}
