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

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function str_replace;

final class StrReplace implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $search;
    private readonly ScalarFunction $replace;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $search
     * @param array<array-key, mixed>|ScalarFunction|string $replace
     */
    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string|array $search,
        ScalarFunction|string|array $replace,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->search = $search instanceof ScalarFunction ? $search : new Literal($search);
        $this->replace = $replace instanceof ScalarFunction ? $replace : new Literal($replace);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['search'] ?? null, ScalarFunction::class),
            $rebuild->of($data['replace'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'search' => $this->search,
            'replace' => $this->replace,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->search, $this->replace];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
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
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $searches = (new Parameter($this->search))->asTypes($rows, $backend, type_string(), type_array());
        $replaces = (new Parameter($this->replace))->asTypes($rows, $backend, type_string(), type_array());
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $search = $searches[$i];
                $replace = $replaces[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('StrReplace function requires non-null value');
                }

                if ($search === null || $replace === null) {
                    throw new InvalidArgumentException('StrReplace function requires non-null search and replace');
                }

                $results[] = str_replace(
                    type_union(type_string(), type_list(type_string()))->assert($search),
                    type_union(type_string(), type_list(type_string()))->assert($replace),
                    $value,
                );
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
