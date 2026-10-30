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
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Value\Json;
use JsonException;
use SensitiveParameter;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_array;
use function is_object;
use function json_encode;

final class JsonEncode implements ScalarFunction
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
        $value = type_bare($this->value->returns());

        return type_optional(
            $value instanceof ListType
            || $value instanceof MapType
            || $value instanceof StructureType
            || $value instanceof ArrayType
            || $value instanceof JsonType
                ? type_json()
                : type_string(),
        );
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
                    $results[] = null;

                    continue;
                }

                try {
                    $encoded = json_encode($value, $flags);

                    if ($encoded === false) {
                        throw new InvalidArgumentException('JsonEncode error: json_encode returned false');
                    }

                    if (is_array($value) || is_object($value)) {
                        $results[] = new Json($encoded);

                        continue;
                    }

                    $results[] = $encoded;
                } catch (JsonException $e) {
                    throw new InvalidArgumentException('JsonEncode error: ' . $e->getMessage());
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
