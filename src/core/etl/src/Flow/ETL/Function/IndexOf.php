<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
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

    public function eval(Row $row, FlowContext $context): ?int
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $needle = (new Parameter($this->needle))->asString($row, $context);
        $offset = type_integer()->assert((new Parameter($this->offset))->as($row, $context, type_integer()));
        $ignoreCase = (new Parameter($this->ignoreCase))->asBoolean($row, $context) ?? false;

        if ($string === null || $needle === null) {
            throw new InvalidArgumentException('IndexOf function requires non-null string and needle');
        }

        if ($ignoreCase) {
            return u($string)->ignoreCase()->indexOf($needle, $offset);
        }

        return u($string)->indexOf($needle, $offset);
    }
}
