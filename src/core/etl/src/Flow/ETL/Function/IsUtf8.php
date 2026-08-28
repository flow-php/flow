<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Symfony\Component\String\b;

final class IsUtf8 implements ScalarFunction
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
        return (new Nullability())->any(type_boolean(), $this->string->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            return null;
        }

        return b($string)->isUtf8();
    }
}
