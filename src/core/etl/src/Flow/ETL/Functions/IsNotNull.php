<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function Flow\Types\DSL\type_boolean;

final class IsNotNull implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(mixed $value)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['value'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_boolean();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $column = $this->value->eval($rows, $backend);

        if ($column->nullCount() === 0) {
            // @mago-ignore analysis:possibly-invalid-argument
            return (new ResultColumn($backend))->of($this, $rows->isEmpty() ? [] : array_fill(0, $rows->count(), true));
        }

        $results = [];

        for ($i = 0, $count = $column->count(); $i < $count; $i++) {
            $results[] = !$column->isNull($i);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
