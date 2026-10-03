<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Symfony\Component\String\u;

final class IndexOf implements ScalarFunction
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
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
        $this->needle = $needle instanceof ScalarFunction ? $needle : lit($needle);
        $this->ignoreCase = $ignoreCase instanceof ScalarFunction ? $ignoreCase : lit($ignoreCase);
        $this->offset = $offset instanceof ScalarFunction ? $offset : lit($offset);
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $needles = (new Parameter($this->needle))->asStrings($rows, $context);
        $offsets = (new Parameter($this->offset))->asTypes($rows, $context, type_integer());
        $ignoreCases = (new Parameter($this->ignoreCase))->asBooleans($rows, $context);
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
                    throw new InvalidArgumentException('IndexOf function requires non-null string and needle');
                }

                if ($ignoreCase) {
                    $results[] = u($string)->ignoreCase()->indexOf($needle, $offset);

                    continue;
                }

                $results[] = u($string)->indexOf($needle, $offset);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
