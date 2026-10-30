<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_map;
use function array_merge;
use function array_slice;
use function array_values;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;

final class ConcatWithSeparator implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<ScalarFunction>
     */
    private readonly array $refs;

    private readonly ScalarFunction $separator;

    public function __construct(ScalarFunction|string $separator, ScalarFunction|string ...$refs)
    {
        $this->separator = $separator instanceof ScalarFunction ? $separator : new Literal($separator);
        $this->refs = array_values(array_map(static fn(ScalarFunction|string $ref): ScalarFunction => $ref
            instanceof ScalarFunction
                ? $ref
                : new Literal($ref), $refs));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['separator'] ?? null, ScalarFunction::class),
            ...$rebuild->all($data['refs'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'separator' => $this->separator,
            'refs' => $this->refs,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->separator, ...$this->refs];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], ...array_slice($children, 1));
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
        $separators = (new Parameter($this->separator))->asStrings($rows, $backend);
        $arguments = array_map(static fn(ScalarFunction $ref): array => (new Parameter($ref))->values(
            $rows,
            $backend,
        ), $this->refs);
        $results = [];
        $i = 0;

        try {
            foreach ($separators as $i => $separator) {
                if (!is_string($separator)) {
                    throw new InvalidArgumentException('ConcatWithSeparator function requires non-null separator');
                }

                /** @var array<string> $concatValues */
                $concatValues = [];

                foreach ($arguments as $argument) {
                    // @mago-ignore analysis:mixed-assignment
                    $value = $argument[$i];

                    if (type_list(type_string())->isValid($value)) {
                        $concatValues = array_merge($concatValues, $value);
                    } elseif ($value !== null) {
                        $concatValues[] = type_string()->cast($value);
                    }
                }

                $results[] = implode($separator, $concatValues);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
