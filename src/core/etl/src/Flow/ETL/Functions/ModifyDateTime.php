<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
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
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\OptionalType;
use SensitiveParameter;

use function Flow\Types\DSL\type_datetime;

final class ModifyDateTime implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $reference;
    private readonly ScalarFunction $modifier;

    public function __construct(mixed $reference, string|ScalarFunction $modifier)
    {
        $this->reference = $reference instanceof ScalarFunction ? $reference : new Literal($reference);
        $this->modifier = $modifier instanceof ScalarFunction ? $modifier : new Literal($modifier);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['reference'] ?? null, ScalarFunction::class),
            $rebuild->of($data['modifier'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'reference' => $this->reference,
            'modifier' => $this->modifier,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->reference, $this->modifier];
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
        $type = $this->reference->returns();
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return $base instanceof DateTimeType ? $base : type_datetime();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->reference))->asInstancesOf($rows, $backend, DateTimeInterface::class);
        $modifiers = (new Parameter($this->modifier))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $modifier = $modifiers[$i];

                if ($modifier === null || $value === null) {
                    throw new InvalidArgumentException('ModifyDateTime function requires non-null values');
                }

                if (!$value instanceof DateTime && !$value instanceof DateTimeImmutable) {
                    throw new InvalidArgumentException(
                        'ModifyDateTime function requires DateTime or DateTimeImmutable object',
                    );
                }

                $results[] = $value->modify($modifier);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
