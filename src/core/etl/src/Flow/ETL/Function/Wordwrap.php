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
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Wordwrap implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $width;
    private readonly ScalarFunction $break;
    private readonly ScalarFunction $cut;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $width,
        ScalarFunction|string $break = "\n",
        ScalarFunction|bool $cut = false,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->width = $width instanceof ScalarFunction ? $width : lit($width);
        $this->break = $break instanceof ScalarFunction ? $break : lit($break);
        $this->cut = $cut instanceof ScalarFunction ? $cut : lit($cut);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->width, $this->break, $this->cut];
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
        return type_string();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $breaks = (new Parameter($this->break))->asStrings($rows, $context);
        $widths = (new Parameter($this->width))->asTypes($rows, $context, type_integer());
        $cuts = (new Parameter($this->cut))->asBooleans($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $break = $breaks[$i];
                $width = $widths[$i];
                $cut = $cuts[$i];

                $width = type_integer()->assert($width);
                $cut ??= false;

                if ($value === null) {
                    throw new InvalidArgumentException('Wordwrap function requires non-null value');
                }

                if ($width <= 0) {
                    $results[] = $value;

                    continue;
                }

                if ($break === null) {
                    $break = "\n";
                }

                $results[] = s($value)->wordwrap($width, $break, $cut)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
