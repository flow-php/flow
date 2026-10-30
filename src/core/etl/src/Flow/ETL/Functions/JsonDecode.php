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
use Flow\Types\Value\Json;
use JsonException;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function is_array;
use function is_string;
use function json_decode;

final class JsonDecode implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $flags;

    public function __construct(
        private readonly ScalarFunction $value,
        ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
        $this->flags = $flags instanceof ScalarFunction ? $flags : new Literal($flags);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['flags'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'flags' => $this->flags,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->flags];
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
        return type_array();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->values($rows, $backend);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                $flags = $flagsList[$i];

                $flags = (int) $flags;

                if ($value === null) {
                    throw new InvalidArgumentException('JsonDecode function requires non-null value');
                }

                if ($value instanceof Json) {
                    $results[] = $value->toArray();

                    continue;
                }

                if (is_array($value)) {
                    $results[] = $value;

                    continue;
                }

                if (!is_string($value)) {
                    throw new InvalidArgumentException('JsonDecode function requires string, array, or Json value');
                }

                try {
                    // @mago-ignore analysis:mixed-assignment
                    $decoded = json_decode($value, true, 512, $flags);
                } catch (JsonException $e) {
                    throw new InvalidArgumentException('JsonDecode error: ' . $e->getMessage(), 0, $e);
                }

                if (!is_array($decoded)) {
                    throw new InvalidArgumentException(
                        'JsonDecode function requires JSON that decodes to an array, cast scalar JSON instead',
                    );
                }

                $results[] = $decoded;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
