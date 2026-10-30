<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function array_filter;
use function array_keys;
use function array_map;
use function array_values;
use function count;
use function Flow\Types\DSL\type_string;
use function implode;
use function max;

final class Concat implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(ScalarFunction|string ...$refs)
    {
        $this->refs = array_values(array_map(static fn(ScalarFunction|string $ref): ScalarFunction => $ref
            instanceof ScalarFunction
                ? $ref
                : new Literal($ref), $refs));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->all($data['refs'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'refs' => $this->refs,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->refs;
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
        return type_string();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $string = type_string();
        /** @var list<list<?string>> $arguments one cast column per argument */
        $arguments = [];

        foreach ($this->refs as $ref) {
            $values = (new Parameter($ref))->values($rows, $backend);

            // a column of strings is taken as it is; anything else is cast once per column, not once per row
            if (
                (count(array_filter($values, 'is_string')) + count(array_keys($values, null, true))) === count($values)
            ) {
                /** @var list<?string> $values */
                $arguments[] = $values;

                continue;
            }

            $cast = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                try {
                    $cast[] = $value === null ? null : $string->cast($value);
                } catch (Exception $e) {
                    throw EvaluationException::at($i, $e);
                }
            }

            $arguments[] = $cast;
        }

        return (new ResultColumn($backend))->of(
            $this,
            $arguments === []
                ? array_fill(0, max(0, $rows->count()), '')
                : array_values(array_map(static fn(?string ...$parts): string => implode('', $parts), ...$arguments)),
        );
    }
}
