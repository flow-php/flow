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

final class StringTitle implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;
    private readonly ScalarFunction $allWords;

    public function __construct(ScalarFunction|string $string, ScalarFunction|bool $allWords = false)
    {
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
        $this->allWords = $allWords instanceof ScalarFunction ? $allWords : lit($allWords);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string, $this->allWords];
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
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $allWords = (new Parameter($this->allWords))->asBoolean($row, $context);

        if ($string === null) {
            throw new InvalidArgumentException('StringTitle function requires non-null value');
        }

        return u($string)->title(allWords: $allWords)->toString();
    }
}
