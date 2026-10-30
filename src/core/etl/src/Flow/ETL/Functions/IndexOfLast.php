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

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Symfony\Component\String\u;

final class IndexOfLast implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $needle;
    private readonly ScalarFunction $ignoreCase;
    private readonly ScalarFunction $offset;

    public function __construct(
        ScalarFunction|string $string,
        ScalarFunction|string $needle,
        ScalarFunction|bool $ignoreCase = false,
        ScalarFunction|int $offset = 0,
    ) {
        $this->string = $string instanceof ScalarFunction ? $string : new Literal($string);
        $this->needle = $needle instanceof ScalarFunction ? $needle : new Literal($needle);
        $this->ignoreCase = $ignoreCase instanceof ScalarFunction ? $ignoreCase : new Literal($ignoreCase);
        $this->offset = $offset instanceof ScalarFunction ? $offset : new Literal($offset);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['needle'] ?? null, ScalarFunction::class),
            $rebuild->of($data['ignore_case'] ?? null, ScalarFunction::class),
            $rebuild->of($data['offset'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'string' => $this->string,
            'needle' => $this->needle,
            'ignore_case' => $this->ignoreCase,
            'offset' => $this->offset,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->needle, $this->ignoreCase, $this->offset];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_integer());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $backend);
        $needles = (new Parameter($this->needle))->asStrings($rows, $backend);
        $offsets = (new Parameter($this->offset))->asTypes($rows, $backend, type_integer());
        $ignoreCases = (new Parameter($this->ignoreCase))->asBooleans($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                $needle = $needles[$i];
                $offset = $offsets[$i];
                $ignoreCase = $ignoreCases[$i];

                $offset = type_integer()->assert($offset);
                $ignoreCase ??= false;

                if ($string === null || $needle === null) {
                    throw new InvalidArgumentException('IndexOfLast function requires non-null string and needle');
                }

                if ($ignoreCase) {
                    $results[] = u($string)->ignoreCase()->indexOfLast($needle, $offset);

                    continue;
                }

                $results[] = u($string)->indexOfLast($needle, $offset);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
