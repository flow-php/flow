<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\MatchCases;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\Selection;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function get_debug_type;
use function is_scalar;
use function sprintf;

final readonly class MatchCondition implements ScalarFunction
{
    private ScalarFunction $condition;

    private ScalarFunction $then;

    public function __construct(mixed $condition, mixed $then)
    {
        $this->condition = $condition instanceof ScalarFunction ? $condition : new Literal($condition);
        $this->then = $then instanceof ScalarFunction ? $then : new Literal($then);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['condition'] ?? null, ScalarFunction::class),
            $rebuild->of($data['then'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'condition' => $this->condition,
            'then' => $this->then,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->condition, $this->then];
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
        return $this->then->returns();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        return $this->then->eval($rows, $backend);
    }

    /**
     * @param list<int> $indices ascending rows of $rows
     *
     * @return list<bool> one per index
     */
    public function valid(Rows $rows, array $indices, Backend $backend): array
    {
        $column = (new Selection($indices))->evaluate($this->condition, $rows, $backend);
        $valid = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->values() as $k => $value) {
            if ($value !== null && !is_scalar($value)) {
                throw new EvaluationException(
                    $indices[$k],
                    new InvalidArgumentException(sprintf('Expected type "boolean", got "%s".', get_debug_type($value))),
                );
            }

            $valid[] = (bool) $value;
        }

        return $valid;
    }
}
