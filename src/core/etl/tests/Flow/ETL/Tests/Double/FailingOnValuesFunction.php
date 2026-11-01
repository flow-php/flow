<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Error;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Parameter;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunctionChain;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_optional;
use function in_array;
use function sprintf;

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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function counters(): self
    {
        return $this->owner ?? $this;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $this->counters()->evals++;
        $this->counters()->seen[] = $rows;
        $values = (new Parameter($this->operand))->asInts($rows, $backend);

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

        return (new ResultColumn($backend))->of($this, $values);
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
