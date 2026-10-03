<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Rows;
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

    public function deterministic(): bool
    {
        return false;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $lengths = (new Parameter($this->length))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($lengths as $i => $length) {
                if ($length === null) {
                    throw new InvalidArgumentException('RandomString requires non-null length');
                }

                $results[] = $this->generator->string($length);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
