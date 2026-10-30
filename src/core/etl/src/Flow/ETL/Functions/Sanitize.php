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
use function mb_strlen;
use function mb_substr;
use function str_repeat;

final class Sanitize implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $placeholder;
    private readonly ScalarFunction $skipCharacters;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|string $placeholder,
        ScalarFunction|int|null $skipCharacters = null,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->placeholder = $placeholder instanceof ScalarFunction ? $placeholder : new Literal($placeholder);
        $this->skipCharacters = $skipCharacters instanceof ScalarFunction
            ? $skipCharacters
            : new Literal($skipCharacters);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['placeholder'] ?? null, ScalarFunction::class),
            $rebuild->of($data['skip_characters'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'placeholder' => $this->placeholder,
            'skip_characters' => $this->skipCharacters,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->placeholder, $this->skipCharacters];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
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
        $vals = (new Parameter($this->value))->asStrings($rows, $backend);
        $placeholders = (new Parameter($this->placeholder))->asStrings($rows, $backend);
        $skipCharactersList = (new Parameter($this->skipCharacters))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($vals as $i => $val) {
                $placeholder = $placeholders[$i];
                $skipCharacters = $skipCharactersList[$i];

                if ($val === null) {
                    throw new InvalidArgumentException('Sanitize function requires non-null value');
                }

                if ($placeholder === null) {
                    throw new InvalidArgumentException('Sanitize function requires non-null placeholder');
                }

                $size = mb_strlen($val);

                if ($skipCharacters !== null && $size > $skipCharacters) {
                    $results[] =
                        mb_substr($val, 0, $skipCharacters) . str_repeat($placeholder, max(0, $size - $skipCharacters));

                    continue;
                }

                $results[] = str_repeat($placeholder, $size);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
