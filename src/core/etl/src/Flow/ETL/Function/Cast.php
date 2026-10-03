<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException as TypesInvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeFactory;

use function array_search;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_is_nullable;
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
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);

        try {
            $resolved = $type instanceof Type ? $type : TypeFactory::fromString($type);
            definition_from_type('cast', $resolved);
        } catch (RuntimeException|TypesInvalidArgumentException|UnsupportedUnionTypeException $e) {
            throw new InvalidArgumentException(
                sprintf('Cast function does not support type: %s', $type instanceof Type ? $type->toString() : $type),
                0,
                $e,
            );
        }

        $this->type = $resolved;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = null;
        $documents = null;

        if (type_bare($this->type) instanceof StringType) {
            $column = (new Parameter($this->value))->column($rows, $context);
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

                return (new ResultColumn($context->backend()))->of($this, (new TextValues())->dateTimes(
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
            foreach ($values ?? (new Parameter($this->value))->values($rows, $context) as $i => $value) {
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

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
