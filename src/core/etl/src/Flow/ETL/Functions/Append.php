<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Append implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $suffix;

    public function __construct(ScalarFunction|string $value, ScalarFunction|string $suffix)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->suffix = $suffix instanceof ScalarFunction ? $suffix : new Literal($suffix);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['suffix'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'suffix' => $this->suffix,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->suffix];
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $suffixes = (new Parameter($this->suffix))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $suffix = $suffixes[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Append function requires non-null value');
                }

                if ($suffix === null) {
                    $results[] = $value;

                    continue;
                }

                $results[] = s($value)->append($suffix)->toString();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
