<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Physical;

use BackedEnum;
use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Layout\BooleanLayout;
use Flow\ETL\Values\Column\Layout\FixedBinary16Layout;
use Flow\ETL\Values\Column\Layout\Float64Layout;
use Flow\ETL\Values\Column\Layout\Int32Layout;
use Flow\ETL\Values\Column\Layout\Int64Layout;
use Flow\ETL\Values\Column\Layout\Layout;
use Flow\ETL\Values\Column\Layout\Utf8Layout;
use Flow\ETL\Values\Column\Php\ListColumnBuilder;
use Flow\ETL\Values\Column\Php\MapColumnBuilder;
use Flow\ETL\Values\Column\Php\NullColumnBuilder;
use Flow\ETL\Values\Column\Php\PhpColumnBuilder;
use Flow\ETL\Values\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Values\Column\Php\StructColumnBuilder;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\DateDefinition;
use Flow\ETL\Values\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Values\Schema\Definition\EnumDefinition;
use Flow\ETL\Values\Schema\Definition\FloatDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\JsonDefinition;
use Flow\ETL\Values\Schema\Definition\ListDefinition;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\StructureDefinition;
use Flow\ETL\Values\Schema\Definition\TimeDefinition;
use Flow\ETL\Values\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Values\Schema\Definition\UuidDefinition;
use Flow\ETL\Values\Schema\Definition\XMLDefinition;
use Flow\ETL\Values\Schema\Definition\XMLElementDefinition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use UnitEnum;

use function array_key_exists;
use function Flow\Types\DSL\type_bare;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function sprintf;
use function strlen;

final readonly class PhysicalFor
{
    private const array DEFINITIONS = [
        BooleanDefinition::class,
        DateDefinition::class,
        DateTimeDefinition::class,
        EnumDefinition::class,
        FloatDefinition::class,
        HTMLDefinition::class,
        HTMLElementDefinition::class,
        IntegerDefinition::class,
        JsonDefinition::class,
        ListDefinition::class,
        MapDefinition::class,
        NullDefinition::class,
        StringDefinition::class,
        StructureDefinition::class,
        TimeDefinition::class,
        TimeZoneDefinition::class,
        UuidDefinition::class,
        XMLDefinition::class,
        XMLElementDefinition::class,
    ];

    /**
     * @param Definition<mixed> $definition
     *
     * @throws ColumnMismatchException
     */
    public function definition(Definition $definition): Physical
    {
        if (!in_array($definition::class, self::DEFINITIONS, true)) {
            throw ColumnMismatchException::unsupportedType(
                $definition->name(),
                $definition->type()->toString(),
                'only the 19 Flow definitions have a column kind',
            );
        }

        try {
            return $this->physical($definition->type());
        } catch (InvalidArgumentException $e) {
            throw ColumnMismatchException::unsupportedType(
                $definition->name(),
                $definition->type()->toString(),
                $e->getMessage(),
            );
        }
    }

    /**
     * A column of $type exists: definition_from_type() accepts it and definition() gives it a Physical.
     *
     * @param Type<mixed> $type
     */
    public function supports(Type $type): bool
    {
        $base = type_bare($type);

        return match (true) {
            $base instanceof ArrayType => true,
            $base instanceof EmptyArrayType,
            $base instanceof PositiveIntegerType,
            $base instanceof NonEmptyStringType,
            $base instanceof ClassStringType,
                => false,
            default => $this->supportsChild($base),
        };
    }

    /**
     * A child (list element, map value, structure element) of $type exists once TypeProjection stored it as json
     * (an array) or as optional (a nullable union): physical() gives it a Physical.
     *
     * @param Type<mixed> $type
     */
    public function supportsChild(Type $type): bool
    {
        $base = type_bare($type);

        return match (true) {
            $base instanceof ArrayType, $base instanceof EmptyArrayType => true,
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof FloatType,
            $base instanceof BooleanType,
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
            $base instanceof DateTimeType,
            $base instanceof DateType,
            $base instanceof TimeType,
            $base instanceof UuidType,
            $base instanceof TimeZoneType,
            $base instanceof JsonType,
            $base instanceof XMLType,
            $base instanceof XMLElementType,
            $base instanceof HTMLType,
            $base instanceof HTMLElementType,
            $base instanceof NullType,
                => true,
            $base instanceof EnumType => $base->class !== UnitEnum::class && $base->class !== BackedEnum::class,
            $base instanceof ListType => $this->supportsChild($base->element()),
            $base instanceof MapType => $this->supportsChild($base->value()),
            $base instanceof StructureType => $this->supportsElements($base),
            default => false,
        };
    }

    /**
     * @param StructureType<mixed> $type
     */
    public function supportsElements(StructureType $type): bool
    {
        foreach ($type->elements() as $element) {
            if (!$this->supportsChild($element->type)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function physical(Type $type): Physical
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof FloatType,
            $base instanceof BooleanType,
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
                => new IdentityPhysical(),
            $base instanceof DateTimeType => new DateTimePhysical($base->zone()),
            $base instanceof DateType => new DatePhysical(),
            $base instanceof TimeType => new TimePhysical(),
            $base instanceof UuidType => new UuidPhysical(),
            $base instanceof TimeZoneType => new TimeZonePhysical(),
            $base instanceof JsonType => new JsonPhysical(),
            $base instanceof EnumType => $base->class === UnitEnum::class || $base->class === BackedEnum::class
                ? throw new InvalidArgumentException('declare the concrete enum class')
                : new EnumPhysical($base->class),
            $base instanceof XMLType => new XmlDocumentPhysical(),
            $base instanceof XMLElementType => new XmlElementPhysical(),
            $base instanceof HTMLType => new HtmlDocumentPhysical(),
            $base instanceof HTMLElementType => new HtmlElementPhysical(),
            $base instanceof ListType => ($element = $this->physical($base->element())) instanceof IdentityPhysical
                ? $element
                : new ListPhysical($element),
            $base instanceof MapType => ($value = $this->physical($base->value())) instanceof IdentityPhysical
                ? $value
                : new MapPhysical($value),
            $base instanceof StructureType => array_filter(
                $elements = $this->elements($base),
                static fn(Physical $element): bool => !$element instanceof IdentityPhysical,
            ) === []
                    ? new IdentityPhysical()
                    : new StructPhysical($elements),
            $base instanceof NullType => new NullPhysical(),
            $base instanceof MixedType => throw new InvalidArgumentException(
                'a mixed element has no column kind, declare it or use json',
            ),
            default => throw new InvalidArgumentException(sprintf('type %s has no column kind', $base->toString())),
        };
    }

    /**
     * @param StructureType<mixed> $type
     *
     * @return array<array-key, Physical>
     */
    public function elements(StructureType $type): array
    {
        $elements = [];

        foreach ($type->elements() as $element) {
            $elements[$element->name] = $this->physical($element->type);
        }

        return $elements;
    }

    /**
     * @param Type<mixed> $type
     */
    public function layout(Type $type): Layout
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof DateTimeType,
            $base instanceof TimeType,
                => new Int64Layout(),
            $base instanceof FloatType => new Float64Layout(),
            $base instanceof DateType => new Int32Layout(),
            $base instanceof BooleanType => new BooleanLayout(),
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
            $base instanceof TimeZoneType,
            $base instanceof JsonType,
            $base instanceof EnumType,
            $base instanceof XMLType,
            $base instanceof XMLElementType,
            $base instanceof HTMLType,
            $base instanceof HTMLElementType,
                => new Utf8Layout(),
            $base instanceof UuidType => new FixedBinary16Layout(),
            default => throw new InvalidArgumentException(sprintf('type %s has no scalar layout', $base->toString())),
        };
    }

    /**
     * The PHP kind a physical value of $type has: the kinds flow_php's builders store, so both backends refuse the
     * same physicals.
     *
     * @param Type<mixed> $type
     */
    public function accepts(Type $type, mixed $physical): bool
    {
        if ($physical === null) {
            return true;
        }

        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof DateTimeType,
            $base instanceof DateType,
            $base instanceof TimeType,
                => is_int($physical),
            $base instanceof FloatType => is_float($physical),
            $base instanceof BooleanType => is_bool($physical),
            $base instanceof UuidType => is_string($physical) && strlen($physical) === 16,
            $base instanceof NullType => true,
            $base instanceof ListType => is_array($physical) && $this->acceptsAll($base->element(), $physical),
            $base instanceof MapType => is_array($physical) && $this->acceptsAll($base->value(), $physical),
            $base instanceof StructureType => is_array($physical) && $this->acceptsElements($base, $physical),
            $base instanceof ArrayType => is_array($physical),
            default => is_string($physical),
        };
    }

    /**
     * @param Type<mixed> $type
     * @param array<array-key, mixed> $physicals
     */
    public function acceptsAll(Type $type, array $physicals): bool
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if (!$this->accepts($type, $physical)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param StructureType<mixed> $type
     * @param array<array-key, mixed> $physical
     */
    public function acceptsElements(StructureType $type, array $physical): bool
    {
        foreach ($type->elements() as $element) {
            if (
                array_key_exists($element->name, $physical)
                && !$this->accepts($element->type, $physical[$element->name])
            ) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function builder(Type $type): PhpColumnBuilder
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        if ($base instanceof ListType) {
            return new ListColumnBuilder($type, $this->builder($base->element()));
        }

        if ($base instanceof MapType) {
            return new MapColumnBuilder($type, $this->builder($base->key()), $this->builder($base->value()));
        }

        if ($base instanceof StructureType) {
            $children = [];

            foreach ($base->elements() as $element) {
                $children[$element->name] = $this->builder($element->type);
            }

            return new StructColumnBuilder($type, $children);
        }

        if ($base instanceof NullType) {
            return new NullColumnBuilder($type);
        }

        return new ScalarColumnBuilder($type, $this->physical($type));
    }
}
