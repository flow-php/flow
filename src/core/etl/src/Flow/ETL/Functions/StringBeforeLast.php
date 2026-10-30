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

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Symfony\Component\String\u;

final class StringBeforeLast implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $needle;
    private readonly ScalarFunction $includeNeedle;

    public function __construct(
        ScalarFunction|string $string,
        ScalarFunction|string $needle,
        ScalarFunction|bool $includeNeedle = false,
    ) {
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
        $this->needle = $needle instanceof ScalarFunction ? $needle : new Literal($needle);
        $this->includeNeedle = $includeNeedle instanceof ScalarFunction ? $includeNeedle : new Literal($includeNeedle);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['needle'] ?? null, ScalarFunction::class),
            $rebuild->of($data['include_needle'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
            'needle' => $this->needle,
            'include_needle' => $this->includeNeedle,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->needle, $this->includeNeedle];
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
        $strings = (new Parameter($this->string))->asStrings($rows, $backend);
        $needles = (new Parameter($this->needle))->asStrings($rows, $backend);
        $includeNeedles = (new Parameter($this->includeNeedle))->asBooleans($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                if ($string === null) {
                    throw new InvalidArgumentException('StringBeforeLast function requires non-null value');
                }

                $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needles[$i]);
                $results[] = u($string)->beforeLast($typedNeedle, $includeNeedles[$i] ?? false)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
