<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
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

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            throw new InvalidArgumentException('StringBeforeLast function requires non-null value');
        }

        $needle = (new Parameter($this->needle))->asString($row, $context) ?? (new Parameter($this->needle))->asArray(
            $row,
            $context,
        );
        $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needle);
        $includeNeedle = (new Parameter($this->includeNeedle))->asBoolean($row, $context);

        return u($string)->beforeLast($typedNeedle, $includeNeedle)->toString();
    }
}
