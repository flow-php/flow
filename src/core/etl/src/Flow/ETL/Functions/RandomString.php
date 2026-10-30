<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;

final class RandomString implements ScalarFunction
{
    private readonly ScalarFunction $length;

    public function __construct(
        ScalarFunction|int $length,
        private readonly RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
    ) {
        $this->length = $length instanceof ScalarFunction ? $length : new Literal($length);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['length'] ?? null, ScalarFunction::class),
            $rebuild->of($data['generator'] ?? null, RandomValueGenerator::class),
        );
    }

    public function describe(): ?Description
    {
        return (
            $this->generator instanceof Describable
                ? Description::compose(self::class, ['length' => $this->length, 'generator' => $this->generator])
                : null
        );
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->length];
    }

    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

        return true;
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $lengths = (new Parameter($this->length))->asInts($rows, $backend);
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
