<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\u;

final class Ascii implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;

    public function __construct(ScalarFunction|string $string)
    {
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
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
            throw new InvalidArgumentException('Ascii function requires non-null value');
        }

        return u($string)->ascii()->toString();
    }
}
