<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Error;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;
use function in_array;
use function sprintf;

/**
 * Passes an integer operand through, failing on the listed values: with the failing row's coordinate
 * (EvaluationException), without one (a plain Exception) or as an Error. Every copy the resolver makes reports its
 * eval() calls and the batches it saw to the instance the test holds.
 */
final class FailingOnValuesFunction implements ScalarFunction
{
    use ScalarFunctionChain;

    public const string WITH_ROW = 'row';

    public const string WITHOUT_ROW = 'batch';

    public const string ERROR = 'error';

    public int $evals = 0;

    /**
     * @var list<Rows>
     */
    public array $seen = [];

    /**
     * @param list<int> $failing
     */
    public function __construct(
        public readonly ScalarFunction $operand,
        public readonly array $failing = [],
        public readonly string $failure = self::WITH_ROW,
        public readonly ?self $owner = null,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->operand];
    }

    public function counters(): self
    {
        return $this->owner ?? $this;
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $this->counters()->evals++;
        $this->counters()->seen[] = $rows;
        $values = (new Parameter($this->operand))->asInts($rows, $context);

        foreach ($values as $i => $value) {
            if (!in_array($value, $this->failing, true)) {
                continue;
            }

            $message = sprintf('fails on %d', $value);

            match ($this->failure) {
                self::WITHOUT_ROW => throw new RuntimeException($message),
                self::ERROR => throw new Error($message),
                default => throw new EvaluationException($i, new RuntimeException($message)),
            };
        }

        return (new ResultColumn($context->backend()))->of($this, $values);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->operand->returns());
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->failing, $this->failure, $this->counters());
    }
}
