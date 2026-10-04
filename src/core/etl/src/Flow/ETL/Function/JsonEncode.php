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
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Value\Json;
use JsonException;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_array;
use function is_object;
use function json_encode;

final class JsonEncode implements ScalarFunction
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
        $value = type_bare($this->value->returns());

        $isContainer =
            $value instanceof ListType
            || $value instanceof MapType
            || $value instanceof StructureType
            || $value instanceof ArrayType
            || $value instanceof JsonType;

        return type_optional($isContainer ? type_json() : type_string());
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
                    $results[] = null;

                    continue;
                }

                try {
                    $encoded = json_encode($value, $flags);

                    if ($encoded === false) {
                        throw new InvalidArgumentException('JsonEncode error: json_encode returned false');
                    }

                    if (is_array($value) || is_object($value)) {
                        $results[] = new Json($encoded);

                        continue;
                    }

                    $results[] = $encoded;
                } catch (JsonException $e) {
                    throw new InvalidArgumentException('JsonEncode error: ' . $e->getMessage());
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
