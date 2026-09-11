<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;

final class RandomString implements ScalarFunction
{
    use ResolvesFromChildren;

    private readonly ScalarFunction $length;

    public function __construct(
        ScalarFunction|int $length,
        private readonly RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
    ) {
        $this->length = $length instanceof ScalarFunction ? $length : lit($length);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->length];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->generator);
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
        $length = (new Parameter($this->length))->asInt($row, $context);

        if ($length === null) {
            throw new InvalidArgumentException('RandomString requires non-null length');
        }

        return $this->generator->string($length);
    }
}
