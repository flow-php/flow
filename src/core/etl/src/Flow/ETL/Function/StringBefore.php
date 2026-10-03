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
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Symfony\Component\String\u;

final class StringBefore implements ScalarFunction
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
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
        $this->needle = $needle instanceof ScalarFunction ? $needle : lit($needle);
        $this->includeNeedle = $includeNeedle instanceof ScalarFunction ? $includeNeedle : lit($includeNeedle);
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $needles = (new Parameter($this->needle))->asStrings($rows, $context);
        $includeNeedles = (new Parameter($this->includeNeedle))->asBooleans($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                if ($string === null) {
                    throw new InvalidArgumentException('StringBefore function requires non-null value');
                }

                $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needles[$i]);
                $results[] = u($string)->before($typedNeedle, $includeNeedles[$i] ?? false)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
