<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Evaluation\Selection;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;
use SensitiveParameter;

use function count;
use function Flow\Types\DSL\type_optional;

final class When implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $condition;

    private readonly ScalarFunction $then;

    /**
     * Null means "no else branch" - never lit(null), so a falsy else value stays distinguishable
     * from an absent one
     */
    private readonly ?ScalarFunction $else;

    public function __construct(mixed $condition, mixed $then, mixed $else = null)
    {
        $this->condition = $condition instanceof ScalarFunction ? $condition : new Literal($condition);
        $this->then = $then instanceof ScalarFunction ? $then : new Literal($then);
        $this->else = $else === null ? null : ($else instanceof ScalarFunction ? $else : new Literal($else));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['condition'] ?? null, ScalarFunction::class),
            $rebuild->of($data['then'] ?? null, ScalarFunction::class),
            ($data['else'] ?? null) === null ? null : $rebuild->of($data['else'], ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'condition' => $this->condition,
            'then' => $this->then,
            'else' => $this->else,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->else === null ? [$this->condition, $this->then] : [$this->condition, $this->then, $this->else];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return count($children) === 3
            ? new self($children[0], $children[1], $children[2])
            : new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $branches = [$this->then->returns()];

        if ($this->else !== null) {
            $branches[] = $this->else->returns();
        }

        $result = (new PromotingUnifier())->unifyAll(
            NullabilityRule::ANY,
            ...$branches,
        ) ?? throw InvalidTypeException::noCommonType(...$branches);

        // No ELSE means a non-matching row yields NULL, as in SQL.
        return $this->else === null ? type_optional($result) : $result;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $then = [];
        $else = [];

        foreach ((new Parameter($this->condition))->asBooleans($rows, $backend) as $i => $condition) {
            if ($condition) {
                $then[] = $i;
            } else {
                $else[] = $i;
            }
        }

        $values = [];

        if ($then !== []) {
            $column = (new Selection($then))->evaluate($this->then, $rows, $backend);

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $value) {
                $values[$then[$k]] = $value;
            }
        }

        if ($else !== [] && $this->else !== null) {
            $column = (new Selection($else))->evaluate($this->else, $rows, $backend);

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $value) {
                $values[$else[$k]] = $value;
            }
        }

        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = $values[$i] ?? null;
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
