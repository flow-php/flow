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
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Truncate implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $length;
    private readonly ScalarFunction $ellipsis;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $length,
        ScalarFunction|string $ellipsis = '...',
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->length = $length instanceof ScalarFunction ? $length : lit($length);
        $this->ellipsis = $ellipsis instanceof ScalarFunction ? $ellipsis : lit($ellipsis);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->length, $this->ellipsis];
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
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $lengths = (new Parameter($this->length))->asInts($rows, $context);
        $ellipsisList = (new Parameter($this->ellipsis))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $length = $lengths[$i];
                $ellipsis = $ellipsisList[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Truncate function requires non-null value');
                }

                if ($length === null) {
                    $results[] = $value;

                    continue;
                }

                $results[] = s($value)->truncate($length, $ellipsis ?? '...')->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
