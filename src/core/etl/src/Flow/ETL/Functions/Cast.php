<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeInterface;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Values\Column\TextValues;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException as TypesInvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeFactory;
use SensitiveParameter;

use function array_search;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function is_string;
use function sprintf;

final class Cast implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var Type<mixed>
     */
    private readonly Type $type;

    private readonly ScalarFunction $value;

    /**
     * The target is resolved and checked here: a string alias becomes a Type, and a type with no
     * Definition arm - not a column - is refused before any row is read.
     *
     * @param string|Type<mixed> $type
     */
    public function __construct(mixed $value, Type|string $type)
    {
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);

        try {
            $resolved = $type instanceof Type ? $type : TypeFactory::fromString($type);
            Definitions::of('cast', $resolved);
        } catch (RuntimeException|TypesInvalidArgumentException|UnsupportedUnionTypeException $e) {
            throw new InvalidArgumentException(
                sprintf('Cast function does not support type: %s', $type instanceof Type ? $type->toString() : $type),
                0,
                $e,
            );
        }

        $this->type = $resolved;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            type_from_array(type_map(type_string(), type_mixed())->assert($data['type'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['value' => $this->value, 'type' => $this->type->normalize()]);
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
        return new self($children[0], $this->type);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = null;
        $documents = null;

        if (type_bare($this->type) instanceof StringType) {
            $column = $this->value->eval($rows, $backend);
            $type = type_bare($column->type());

            if ($type instanceof DateTimeType || $type instanceof DateType) {
                /** @var list<?int> $physicals */
                $physicals = $column->physicals();

                if ($column->nullCount() > 0 && !type_is_nullable($this->type)) {
                    throw EvaluationException::at(
                        (int) array_search(null, $physicals, true),
                        new InvalidArgumentException('Cast function requires non-null value'),
                    );
                }

                return (new ResultColumn($backend))->of($this, (new TextValues())->dateTimes(
                    $type,
                    $physicals,
                    DateTimeInterface::RFC3339,
                ));
            }

            if ($type instanceof XMLType) {
                $values = $column->physicals();
                $documents = new XmlDocumentPhysical();
            }
        }

        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values ?? (new Parameter($this->value))->values($rows, $backend) as $i => $value) {
                if ($documents !== null && is_string($value)) {
                    // @mago-ignore analysis:mixed-assignment
                    $value = $documents->text($value) ?? $documents->fromPhysical($value);
                }

                if (null === $value && !type_is_nullable($this->type)) {
                    throw new InvalidArgumentException('Cast function requires non-null value');
                }

                try {
                    $results[] = $this->type->cast($value);
                } catch (CastingException $e) {
                    throw new InvalidArgumentException('Cast function failed: ' . $e->getMessage(), 0, $e);
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
