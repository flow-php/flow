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
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function array_key_exists;
use function array_map;
use function array_values;
use function Flow\Types\DSL\type_boolean;
use function range;

final readonly class Any implements ScalarFunction
{
    /**
     * @var array<ScalarFunction>
     */
    private array $functions;

    public function __construct(ScalarFunction ...$functions)
    {
        $this->functions = $functions;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->all($data['functions'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'functions' => $this->functions,
        ]);
    }

    public function and(ScalarFunction $scalarFunction): All
    {
        return new All(...$this->functions, ...[$scalarFunction]);
    }

    public function andNot(ScalarFunction $scalarFunction): All
    {
        return new All(...$this->functions, ...[new Not($scalarFunction)]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return array_values($this->functions);
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
        return new self(...$children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), ...array_map(
            static fn(ScalarFunction $function): Type => $function->returns(),
            array_values($this->functions),
        ));
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $undecided = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $results = [];

        /** @var array<int, true> $sawNull */
        $sawNull = [];

        foreach ($this->functions as $function) {
            if ($undecided === []) {
                break;
            }

            $column = (new Selection($undecided))->evaluate($function, $rows, $backend);
            $next = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $k => $value) {
                $i = $undecided[$k];

                if ($value === null) {
                    $sawNull[$i] = true;
                    $next[] = $i;

                    continue;
                }

                if ($value) {
                    $results[$i] = true;

                    continue;
                }

                $next[] = $i;
            }

            $undecided = $next;
        }

        foreach ($undecided as $i) {
            $results[$i] = array_key_exists($i, $sawNull) ? null : false;
        }

        $ordered = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $ordered[] = $results[$i];
        }

        return (new ResultColumn($backend))->of($this, $ordered);
    }

    public function or(ScalarFunction $scalarFunction): self
    {
        return new self(...$this->functions, ...[$scalarFunction]);
    }

    public function orNot(ScalarFunction $scalarFunction): self
    {
        return new self(...$this->functions, ...[new Not($scalarFunction)]);
    }
}
