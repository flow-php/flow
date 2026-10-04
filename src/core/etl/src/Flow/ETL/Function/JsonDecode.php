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
use Flow\Types\Value\Json;
use JsonException;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function is_array;
use function is_string;
use function json_decode;

final class JsonDecode implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $flags;

    public function __construct(
        private readonly ScalarFunction $value,
        ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
        $this->flags = $flags instanceof ScalarFunction ? $flags : lit($flags);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->flags];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_array();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->values($rows, $context);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                $flags = $flagsList[$i];

                $flags = (int) $flags;

                if ($value === null) {
                    throw new InvalidArgumentException('JsonDecode function requires non-null value');
                }

                if ($value instanceof Json) {
                    $results[] = $value->toArray();

                    continue;
                }

                if (is_array($value)) {
                    $results[] = $value;

                    continue;
                }

                if (!is_string($value)) {
                    throw new InvalidArgumentException('JsonDecode function requires string, array, or Json value');
                }

                try {
                    // @mago-ignore analysis:mixed-assignment
                    $decoded = json_decode($value, true, 512, $flags);
                } catch (JsonException $e) {
                    throw new InvalidArgumentException('JsonDecode error: ' . $e->getMessage(), 0, $e);
                }

                if (!is_array($decoded)) {
                    throw new InvalidArgumentException(
                        'JsonDecode function requires JSON that decodes to an array, cast scalar JSON instead',
                    );
                }

                $results[] = $decoded;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
