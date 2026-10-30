<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\Trim\Type;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type as FlowType;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;
use function ltrim;
use function rtrim;
use function trim;

final class Trim implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    private readonly ScalarFunction $type;

    private readonly ScalarFunction $characters;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|Type $type = Type::BOTH,
        ScalarFunction|string $characters = " \t\n\r\0\x0B",
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->type = $type instanceof ScalarFunction ? $type : new Literal($type);
        $this->characters = $characters instanceof ScalarFunction ? $characters : new Literal($characters);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['type'] ?? null, ScalarFunction::class),
            $rebuild->of($data['characters'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'type' => $this->type,
            'characters' => $this->characters,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->type, $this->characters];
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
     * @return FlowType<mixed>
     */
    public function returns(): FlowType
    {
        return type_string();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $backend);
        $types = (new Parameter($this->type))->asEnums($rows, $backend, Type::class);
        $charactersList = (new Parameter($this->characters))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $type = $types[$i];
                $characters = $charactersList[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Trim function requires non-null value');
                }

                if ($type === null || $characters === null) {
                    throw new InvalidArgumentException('Trim function requires non-null type and characters');
                }

                $results[] = match ($type) {
                    Type::LEFT => ltrim($value, $characters),
                    Type::RIGHT => rtrim($value, $characters),
                    Type::BOTH => trim($value, $characters),
                };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
