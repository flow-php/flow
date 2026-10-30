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
use function str_pad;

final class StrPad implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $length;
    private readonly ScalarFunction $padString;
    private readonly ScalarFunction $type;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|int $length,
        ScalarFunction|string $padString = ' ',
        ScalarFunction|int $type = STR_PAD_RIGHT,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->length = $length instanceof ScalarFunction ? $length : new Literal($length);
        $this->padString = $padString instanceof ScalarFunction ? $padString : new Literal($padString);
        $this->type = $type instanceof ScalarFunction ? $type : new Literal($type);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['length'] ?? null, ScalarFunction::class),
            $rebuild->of($data['pad_string'] ?? null, ScalarFunction::class),
            $rebuild->of($data['type'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'length' => $this->length,
            'pad_string' => $this->padString,
            'type' => $this->type,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->length, $this->padString, $this->type];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
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
        $lengths = (new Parameter($this->length))->asInts($rows, $backend);
        $padStrings = (new Parameter($this->padString))->asStrings($rows, $backend);
        $types = (new Parameter($this->type))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $length = $lengths[$i];
                $padString = $padStrings[$i];
                $type = $types[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('StrPad function requires non-null value');
                }

                if ($length === null || $padString === null || $type === null) {
                    throw new InvalidArgumentException('StrPad function requires non-null length, padString and type');
                }

                $results[] = str_pad($value, $length, $padString, $type);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
