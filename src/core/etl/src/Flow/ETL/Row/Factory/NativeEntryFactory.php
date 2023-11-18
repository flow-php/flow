<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\DSL\Entry as EntryDSL;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Schema;

/**
 * @implements EntryFactory<array>
 */
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
     * @throws \JsonException
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
                        return new Row\Entry\JsonEntry($entryName, $value);
                    }

                    if ($this->isUuid($trimmedValue)) {
                        return new Row\Entry\UuidEntry($entryName, Entry\Type\Uuid::fromString($value));
                    }

                    if ($this->isXML($trimmedValue)) {
                        return new Entry\XMLEntry($entryName, $value);
                    }
                }

                return new Row\Entry\StringEntry($entryName, $value);
            }

            if ($valueType->isFloat()) {
                return new Row\Entry\FloatEntry($entryName, $value);
            }

            if ($valueType->isInteger()) {
                return new Row\Entry\IntegerEntry($entryName, $value);
            }

            if ($valueType->isBoolean()) {
                return new Row\Entry\BooleanEntry($entryName, $value);
            }
        }

        if ($valueType instanceof ObjectType) {
            if ($valueType->class === \DOMDocument::class) {
                return new Row\Entry\XMLEntry($entryName, $value);
            }

            if (\in_array($valueType->class, [\DOMElement::class, \DOMNode::class], true)) {
                return new Row\Entry\XMLNodeEntry($entryName, $value);
            }

            if (\in_array($valueType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                return new Row\Entry\DateTimeEntry($entryName, $value);
            }

            if (\in_array($valueType->class, [Entry\Type\Uuid::class, \Ramsey\Uuid\UuidInterface::class, \Symfony\Component\Uid\Uuid::class], true)) {
                if (\in_array($valueType->class, [\Ramsey\Uuid\UuidInterface::class, \Symfony\Component\Uid\Uuid::class], true)) {
                    return new Row\Entry\UuidEntry($entryName, new Entry\Type\Uuid($value));
                }

                return new Row\Entry\UuidEntry($entryName, $value);
            }

            return new Row\Entry\ObjectEntry($entryName, $value);
        }

        if ($valueType instanceof ArrayType) {
            return new Row\Entry\ArrayEntry($entryName, $value);
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

        $type = \gettype($value);

        throw new InvalidArgumentException("{$type} can't be converted to any known Entry");
    }

    private function fromDefinition(Schema\Definition $definition, mixed $value) : Entry
    {
        if ($definition->isNullable() && null === $value) {
            return EntryDSL::null($definition->entry()->name());
        }

        try {
            foreach ($definition->types() as $type) {
                if ($type === Entry\StringEntry::class) {
                    return EntryDSL::string($definition->entry()->name(), $value);
                }

                if ($type === Entry\IntegerEntry::class) {
                    return EntryDSL::integer($definition->entry()->name(), $value);
                }

                if ($type === Entry\FloatEntry::class) {
                    return EntryDSL::float($definition->entry()->name(), $value);
                }

                if ($type === Entry\BooleanEntry::class) {
                    return EntryDSL::boolean($definition->entry()->name(), $value);
                }

                if ($type === Entry\XMLEntry::class) {
                    return EntryDSL::xml($definition->entry()->name(), $value);
                }

                if ($type === Entry\UuidEntry::class) {
                    return EntryDSL::uuid($definition->entry()->name(), $value);
                }

                if ($type === Entry\ObjectEntry::class) {
                    return EntryDSL::object($definition->entry()->name(), $value);
                }

                if ($type === Entry\DateTimeEntry::class) {
                    return EntryDSL::datetime($definition->entry()->name(), $value);
                }

                if ($type === Entry\EnumEntry::class) {
                    /** @var class-string<\UnitEnum> $enumClass */
                    $enumClass = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CLASS);
                    /** @var array<\UnitEnum> $cases */
                    $cases = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CASES);

                    foreach ($cases as $case) {
                        if ($case->name === $value) {
                            return EntryDSL::enum($definition->entry()->name(), $case);
                        }
                    }

                    throw new InvalidArgumentException("Value \"{$value}\" can't be converted to " . $enumClass . ' enum');
                }

                if ($type === Entry\JsonEntry::class) {
                    try {
                        return EntryDSL::json_object($definition->entry()->name(), $value);
                    } catch (InvalidArgumentException) {
                        return EntryDSL::json($definition->entry()->name(), $value);
                    }
                }

                if ($type === Entry\ArrayEntry::class) {
                    return EntryDSL::array($definition->entry()->name(), $value);
                }

                if ($type === Entry\MapEntry::class) {
                    /** @var MapType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_MAP_ENTRY_TYPE);

                    return EntryDSL::map($definition->entry()->name(), $value, $entryType);
                }

                if ($type === Entry\StructureEntry::class) {
                    /** @var StructureType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE);

                    return EntryDSL::structure($definition->entry()->name(), $value, $entryType);
                }

                if ($type === Entry\ListEntry::class) {
                    /** @var ListType $entryType */
                    $entryType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

                    $elementType = $entryType->element();

                    if ($elementType->type() instanceof ObjectType) {
                        /** @var mixed $firstValue */
                        $firstValue = \current($value);

                        if (\is_a($elementType->type()->class, \DateTimeInterface::class, true) && \is_string($firstValue)) {
                            return new Entry\ListEntry(
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
