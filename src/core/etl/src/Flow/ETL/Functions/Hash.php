<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Kernels\Hash\Algorithm;
use Flow\ETL\Kernels\Hash\NativePHPHash;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use SensitiveParameter;

use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_scalar;
use function serialize;

final class Hash implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(
        mixed $value,
        private readonly Algorithm $algorithm = new NativePHPHash(),
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['algorithm'] ?? null, Algorithm::class),
        );
    }

    public function describe(): ?Description
    {
        return (
            $this->algorithm instanceof Describable
                ? Description::compose(self::class, ['value' => $this->value, 'algorithm' => $this->algorithm])
                : null
        );
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->algorithm);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->values($rows, $backend);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                if ($value instanceof Json) {
                    $value = $value->toArray();
                }

                $results[] = match ($value) {
                    null => null,
                    default => match (gettype($value)) {
                        'array', 'object' => $this->algorithm->hash(serialize($value)),
                        default => $this->algorithm->hash(is_scalar($value) ? (string) $value : ''),
                    },
                };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
