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

use function array_map;
use function array_values;
use function range;

final class Coalesce implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $values;

    public function __construct(ScalarFunction ...$values)
    {
        $this->values = array_values($values);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->all($data['values'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'values' => $this->values,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->values;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self(...$children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $branches = array_map(static fn(ScalarFunction $value): Type => $value->returns(), $this->values);

        // SQL COALESCE is nullable only when every branch is - a null in one branch falls through.
        return (
            (new PromotingUnifier())->unifyAll(
                NullabilityRule::ALL,
                ...$branches,
            ) ?? throw InvalidTypeException::noCommonType(...$branches)
        );
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $pending = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $values = [];

        foreach ($this->values as $value) {
            if ($pending === []) {
                break;
            }

            $column = (new Selection($pending))->evaluate($value, $rows, $backend);
            $stillNull = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $result) {
                if ($result === null) {
                    $stillNull[] = $pending[$k];

                    continue;
                }

                $values[$pending[$k]] = $result;
            }

            $pending = $stillNull;
        }

        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $results[] = $values[$i] ?? null;
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
