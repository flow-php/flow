<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Json;
use SensitiveParameter;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_scalar;
use function serialize;

final class Hash implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly HashAlgorithm $algorithm;

    private readonly ScalarFunction $value;

    public function __construct(mixed $value, HashAlgorithm $algorithm = HashAlgorithm::xxh128)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->algorithm = $algorithm;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            HashAlgorithm::named(type_string()->assert($data['algorithm'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['value' => $this->value, 'algorithm' => $this->algorithm->value]);
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
        $column = $this->value->eval($rows, $backend);

        return $backend->hash(
            type_bare($column->type()) instanceof StringType ? $column : $this->texts($column, $backend),
            $this->algorithm,
        );
    }

    private function texts(Column $column, Backend $backend): Column
    {
        $texts = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $i => $value) {
                if ($value instanceof Json) {
                    $value = $value->toArray();
                }

                $texts[] = $value === null
                    ? null
                    : match (gettype($value)) {
                        'array', 'object' => serialize($value),
                        default => is_scalar($value) ? (string) $value : '',
                    };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $texts);
    }
}
