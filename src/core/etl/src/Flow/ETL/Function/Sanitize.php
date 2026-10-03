<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
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
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->placeholder = $placeholder instanceof ScalarFunction ? $placeholder : lit($placeholder);
        $this->skipCharacters = $skipCharacters instanceof ScalarFunction ? $skipCharacters : lit($skipCharacters);
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $vals = (new Parameter($this->value))->asStrings($rows, $context);
        $placeholders = (new Parameter($this->placeholder))->asStrings($rows, $context);
        $skipCharactersList = (new Parameter($this->skipCharacters))->asInts($rows, $context);
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

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
