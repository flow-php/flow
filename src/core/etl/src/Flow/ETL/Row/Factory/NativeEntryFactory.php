<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\obj_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_node_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Logical\JsonType;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Logical\XMLNodeType;
use Flow\ETL\PHP\Type\Logical\XMLType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Schema;
use Ramsey\Uuid\UuidInterface;
use Symfony\Component\Uid\Uuid;

final class NativeEntryFactory implements EntryFactory
{
    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public function create(string $entryName, mixed $value, ?Schema $schema = null) : Entry
    {
        if ($schema !== null) {
            return $this->fromDefinition($schema->getDefinition($entryName), $value);
        }

        if (null === $value) {
            return new Entry\NullEntry($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        if ($valueType instanceof ScalarType) {
            if ($valueType->isString()) {
                $stringChecker = new StringTypeChecker($value);

                if ($stringChecker->isJson()) {
                    return json_entry($entryName, $value);
                }

                if ($stringChecker->isUuid()) {
                    return uuid_entry($entryName, Entry\Type\Uuid::fromString($value));
                }

                if ($stringChecker->isXML()) {
                    return xml_entry($entryName, $value);
                }

                return str_entry($entryName, $value);
            }

            if ($valueType->isFloat()) {
                return float_entry($entryName, $value);
            }

            if ($valueType->isInteger()) {
                return int_entry($entryName, $value);
            }

            if ($valueType->isBoolean()) {
                return bool_entry($entryName, $value);
            }
        }

        if ($valueType instanceof JsonType) {
            return json_entry($entryName, $value);
        }

        if ($valueType instanceof UuidType) {
            return uuid_entry($entryName, $value);
        }

        if ($valueType instanceof DateTimeType) {
            return datetime_entry($entryName, $value);
        }

        if ($valueType instanceof XMLType) {
            return xml_entry($entryName, $value);
        }

        if ($valueType instanceof XMLNodeType) {
            return xml_node_entry($entryName, $value);
        }

        if ($valueType instanceof ObjectType) {
            if ($valueType->class === \DOMDocument::class) {
                return xml_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [\DOMElement::class, \DOMNode::class], true)) {
                return xml_node_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                return datetime_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [Entry\Type\Uuid::class, UuidInterface::class, Uuid::class], true)) {
                if (\in_array($valueType->class, [UuidInterface::class, Uuid::class], true)) {
                    return uuid_entry($entryName, new Entry\Type\Uuid($value));
                }

                return uuid_entry($entryName, $value);
            }

            return object_entry($entryName, $value);
        }

        if ($valueType instanceof EnumType) {
            return enum_entry($entryName, $value);
        }

        if ($valueType instanceof ArrayType) {
            return array_entry($entryName, $value);
        }

        if ($valueType instanceof ListType) {
            return new Entry\ListEntry($entryName, $value, $valueType);
        }

        if ($valueType instanceof MapType) {
            return new Entry\MapEntry($entryName, $value, $valueType);
        }

        if ($valueType instanceof StructureType) {
            return new Entry\StructureEntry($entryName, $value, $valueType);
        }

        throw new InvalidArgumentException("{$valueType->toString()} can't be converted to any known Entry");
    }

    private function fromDefinition(Schema\Definition $definition, mixed $value) : Entry
    {
        if ($definition->isNullable() && null === $value) {
            return null_entry($definition->entry()->name());
        }

        try {
            if ($definition->type() instanceof ScalarType) {
                return match ($definition->type()->type()) {
                    ScalarType::STRING => str_entry($definition->entry()->name(), $value),
                    ScalarType::INTEGER => int_entry($definition->entry()->name(), $value),
                    ScalarType::FLOAT => float_entry($definition->entry()->name(), $value),
                    ScalarType::BOOLEAN => bool_entry($definition->entry()->name(), $value),
                    default => throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\""),
                };
            }

            if ($definition->type() instanceof XMLType) {
                return xml_entry($definition->entry()->name(), $value);
            }

            if ($definition->type() instanceof UuidType) {
                return uuid_entry($definition->entry()->name(), $value);
            }

            if ($definition->type() instanceof ObjectType) {
                return obj_entry($definition->entry()->name(), $value);
            }

            if ($definition->type() instanceof DateTimeType) {
                return datetime_entry($definition->entry()->name(), $value);
            }

            if ($definition->type() instanceof EnumType) {
                /** @var class-string<\UnitEnum> $enumClass */
                $enumClass = $definition->type()->class;
                /** @var array<\UnitEnum> $cases */
                $cases = $definition->type()->class::cases();

                foreach ($cases as $case) {
                    if ($case->name === $value) {
                        return enum_entry($definition->entry()->name(), $case);
                    }
                }

                throw new InvalidArgumentException("Value \"{$value}\" can't be converted to " . $enumClass . ' enum');
            }

            if ($definition->type() instanceof JsonType) {
                try {
                    return json_object_entry($definition->entry()->name(), $value);
                } catch (InvalidArgumentException) {
                    return json_entry($definition->entry()->name(), $value);
                }
            }

            if ($definition->type() instanceof ArrayType) {
                return array_entry($definition->entry()->name(), $value);
            }

            if ($definition->type() instanceof MapType) {
                return map_entry($definition->entry()->name(), $value, $definition->type());
            }

            if ($definition->type() instanceof StructureType) {
                return struct_entry($definition->entry()->name(), $value, $definition->type());
            }

            if ($definition->type() instanceof ListType) {
                return new Entry\ListEntry($definition->entry()->name(), $value, $definition->type());
            }
        } catch (InvalidArgumentException|\TypeError $e) {
            throw new InvalidArgumentException("Field \"{$definition->entry()}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\"");
    }
}
