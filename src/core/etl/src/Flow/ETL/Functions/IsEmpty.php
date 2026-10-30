<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;
use function Symfony\Component\String\s;

final class IsEmpty implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(ScalarFunction|string $value)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['value'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
        ]);
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
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), $this->value->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                if ($value === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = s($value)->isEmpty();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
