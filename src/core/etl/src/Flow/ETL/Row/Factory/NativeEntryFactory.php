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
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\obj_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_node_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Schema;
use Ramsey\Uuid\UuidInterface;
use Symfony\Component\Uid\Uuid;

final class NativeEntryFactory implements EntryFactory
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

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
            return new Row\Entry\NullEntry($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        if ($valueType instanceof ScalarType) {
            if ($valueType->isString()) {
                $trimmedValue = \trim($value);

                if ('' !== $trimmedValue) {
                    if ($this->isJson($trimmedValue)) {
                        return json_entry($entryName, $value);
                    }

                    if ($this->isUuid($trimmedValue)) {
                        return uuid_entry($entryName, Entry\Type\Uuid::fromString($value));
                    }

                    if ($this->isXML($trimmedValue)) {
                        return xml_entry($entryName, $value);
                    }
                }

                return string_entry($entryName, $value);
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
            return new Row\Entry\MapEntry($entryName, $value, $valueType);
        }

        if ($valueType instanceof StructureType) {
            return new Row\Entry\StructureEntry($entryName, $value, $valueType);
        }

        throw new InvalidArgumentException("{$valueType->toString()} can't be converted to any known Entry");
    }

    private function fromDefinition(Schema\Definition $definition, mixed $value) : Entry
    {
        if ($definition->isNullable() && null === $value) {
            return null_entry($definition->entry()->name());
        }

        try {
            foreach ($definition->types() as $type) {
                if ($type === Entry\StringEntry::class) {
                    return string_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\IntegerEntry::class) {
                    return int_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\FloatEntry::class) {
                    return float_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\BooleanEntry::class) {
                    return bool_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\XMLEntry::class) {
                    return xml_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\UuidEntry::class) {
                    return uuid_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\ObjectEntry::class) {
                    return obj_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\DateTimeEntry::class) {
                    return datetime_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\EnumEntry::class) {
                    /** @var class-string<\UnitEnum> $enumClass */
                    $enumClass = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CLASS);
                    /** @var array<\UnitEnum> $cases */
                    $cases = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CASES);

                    foreach ($cases as $case) {
                        if ($case->name === $value) {
                            return enum_entry($definition->entry()->name(), $case);
                        }
                    }

                    throw new InvalidArgumentException("Value \"{$value}\" can't be converted to " . $enumClass . ' enum');
                }

                if ($type === Entry\JsonEntry::class) {
                    try {
                        return json_object_entry($definition->entry()->name(), $value);
                    } catch (InvalidArgumentException) {
                        return json_entry($definition->entry()->name(), $value);
                    }
                }

                if ($type === Entry\ArrayEntry::class) {
                    return array_entry($definition->entry()->name(), $value);
                }

                if ($type === Entry\MapEntry::class) {
                    /** @var MapType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_MAP_ENTRY_TYPE);

                    return map_entry($definition->entry()->name(), $value, $entryType);
                }

                if ($type === Entry\StructureEntry::class) {
                    /** @var StructureType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

                    return struct_entry($definition->entry()->name(), $value, $entryType);
                }

                if ($type === Entry\ListEntry::class) {
                    /** @var ListType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

                    $elementType = $entryType->element();

                    if ($elementType->type() instanceof ObjectType) {
                        /** @var mixed $firstValue */
                        $firstValue = \current($value);

                        if (\is_a($elementType->type()->class, \DateTimeInterface::class, true) && \is_string($firstValue)) {
                            return list_entry(
                                $definition->entry()->name(),
                                \array_map(static fn (string $datetime) : \DateTimeImmutable => new \DateTimeImmutable($datetime), $value),
                                $entryType,
                            );
                        }
                    }

                    return new Entry\ListEntry($definition->entry()->name(), $value, $entryType);
                }
            }
        } catch (InvalidArgumentException|\TypeError $e) {
            throw new InvalidArgumentException("Field \"{$definition->entry()}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\"");
    }

    private function isJson(string $string) : bool
    {
        if ('{' !== $string[0] && '[' !== $string[0]) {
            return false;
        }

        if (
            (!\str_starts_with($string, '{') || !\str_ends_with($string, '}'))
            && (!\str_starts_with($string, '[') || !\str_ends_with($string, ']'))
        ) {
            return false;
        }

        try {
            return \is_array(\json_decode($string, true, flags: \JSON_THROW_ON_ERROR));
        } catch (\Exception) {
            return false;
        }
    }

    private function isUuid(string $string) : bool
    {
        if (\strlen($string) !== 36) {
            return false;
        }

        return 0 !== \preg_match(Entry\Type\Uuid::UUID_REGEXP, $string);
    }

    private function isXML(string $string) : bool
    {
        if ('<' !== $string[0]) {
            return false;
        }

        if (\preg_match('/<(.+?)>(.+?)<\/(.+?)>/', $string) === 1) {
            try {
                \libxml_use_internal_errors(true);

                $doc = new \DOMDocument();
                $result = $doc->loadXML($string);
                \libxml_clear_errors(); // Clear any errors if needed
                \libxml_use_internal_errors(false); // Restore standard error handling

                /** @psalm-suppress RedundantCastGivenDocblockType */
                return (bool) $result;
            } catch (\Exception) {
                \libxml_clear_errors(); // Clear any errors if needed
                \libxml_use_internal_errors(false); // Restore standard error handling

                return false;
            }
        }

        return false;
    }
}
