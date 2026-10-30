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
use Symfony\Component\String\AbstractString;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Chunk implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $size;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $size)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->size = $size instanceof ScalarFunction ? $size : new Literal($size);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['size'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'size' => $this->size,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->size];
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
        return type_list(type_string());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $sizes = (new Parameter($this->size))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $size = $sizes[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Chunk function requires non-null value');
                }

                if ($size === null || $size <= 0) {
                    throw new InvalidArgumentException('Chunk function requires non-null, positive size');
                }

                $results[] = array_map(
                    static fn(AbstractString $chunk): string => $chunk->toString(),
                    iterator_to_array(s($value)->chunk($size), false),
                );
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
