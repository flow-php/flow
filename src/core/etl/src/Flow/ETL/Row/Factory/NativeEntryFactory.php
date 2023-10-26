<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\DSL\Entry as EntryDSL;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\Type;
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

        if (\is_string($value)) {
            if ('' !== $value) {
                $trimmedValue = \trim($value);

                if ($this->isJson($trimmedValue)) {
                    return Row\Entry\JsonEntry::fromJsonString($entryName, $value);
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

        if (\is_float($value)) {
            return new Row\Entry\FloatEntry($entryName, $value);
        }

        if (\is_int($value)) {
            return new Row\Entry\IntegerEntry($entryName, $value);
        }

        if (\is_bool($value)) {
            return new Row\Entry\BooleanEntry($entryName, $value);
        }

        if (\is_object($value)) {
            if ($value instanceof \DOMDocument) {
                return new Row\Entry\XMLEntry($entryName, $value);
            }

            if ($value instanceof \DOMNode) {
                return new Row\Entry\XMLNodeEntry($entryName, $value);
            }

            if ($value instanceof \DateTimeImmutable) {
                return new Row\Entry\DateTimeEntry($entryName, $value);
            }

            if ($value instanceof Entry\Type\Uuid || $value instanceof \Ramsey\Uuid\UuidInterface || $value instanceof \Symfony\Component\Uid\Uuid) {
                if ($value instanceof \Ramsey\Uuid\UuidInterface || $value instanceof \Symfony\Component\Uid\Uuid) {
                    return new Row\Entry\UuidEntry($entryName, new Entry\Type\Uuid($value));
                }

                return new Row\Entry\UuidEntry($entryName, $value);
            }

            return new Row\Entry\ObjectEntry($entryName, $value);
        }

        if (\is_array($value)) {
            if ([] === $value) {
                return new Row\Entry\ArrayEntry($entryName, $value);
            }

            if ($this->isStructure($value)) {
                return $this->createStructureEntryFromArray($entryName, $value);
            }

            if (!\array_is_list($value)) {
                return new Row\Entry\ArrayEntry($entryName, $value);
            }

            $type = null;
            $class = null;

            foreach ($value as $valueElement) {
                if ($type === null) {
                    $type = \gettype($valueElement);
                }

                if ($type === 'object' && $class === null) {
                    /** @var object $valueElement */
                    $class = $this->getClass($valueElement);
                }

                if ($type !== \gettype($valueElement)) {
                    return new Row\Entry\ArrayEntry($entryName, $value);
                }

                /** @var object $valueElement */
                if ($class !== null && $class !== $this->getClass($valueElement)) {
                    return new Row\Entry\ArrayEntry($entryName, $value);
                }
            }

            if ($type === 'array') {
                return new Row\Entry\ArrayEntry($entryName, $value);
            }

            if ($class !== null) {
                if ($class === \DateTimeImmutable::class || $class === \DateTime::class) {
                    $class = \DateTimeInterface::class;
                }

                if ($class === \DOMElement::class) {
                    $class = \DOMNode::class;
                }

                return new Entry\ListEntry($entryName, Entry\TypedCollection\ObjectType::of($class), $value);
            }

            return new Entry\ListEntry($entryName, Entry\TypedCollection\ScalarType::fromString($type), $value);
        }

        if (null === $value) {
            return new Row\Entry\NullEntry($entryName);
        }

        $type = \gettype($value);

        throw new InvalidArgumentException("{$type} can't be converted to any known Entry");
    }

    private function createStructureEntryFromArray(string $entryName, array $array) : Row\Entry\StructureEntry
    {
        $structureEntries = [];

        foreach ($array as $key => $value) {
            if (\is_array($value)) {
                $structureEntries[] = $this->createStructureEntryFromArray($key, $value);
            } else {
                $structureEntries[] = $this->create($key, $value);
            }
        }

        return new Row\Entry\StructureEntry($entryName, ...$structureEntries);
    }

    private function fromDefinition(Schema\Definition $definition, mixed $value) : Entry
    {
        if ($definition->isNullable() && null === $value) {
            return EntryDSL::null($definition->entry()->name());
        }

        foreach ($definition->types() as $type) {
            if ($type === Entry\StringEntry::class && \is_string($value)) {
                return EntryDSL::string($definition->entry()->name(), $value);
            }

            if ($type === Entry\IntegerEntry::class && \is_int($value)) {
                return EntryDSL::integer($definition->entry()->name(), $value);
            }

            if ($type === Entry\FloatEntry::class && \is_float($value)) {
                return EntryDSL::float($definition->entry()->name(), $value);
            }

            if ($type === Entry\BooleanEntry::class && \is_bool($value)) {
                return EntryDSL::boolean($definition->entry()->name(), $value);
            }

            if ($type === Entry\JsonEntry::class && \is_string($value)) {
                return EntryDSL::json_string($definition->entry()->name(), $value);
            }

            if ($type === Entry\JsonEntry::class && \is_array($value)) {
                try {
                    return EntryDSL::json_object($definition->entry()->name(), $value);
                } catch (InvalidArgumentException $e) {
                    return EntryDSL::json($definition->entry()->name(), $value);
                }
            }

            if ($type === Entry\XMLEntry::class && (\is_string($value) || $value instanceof \DOMDocument)) {
                return EntryDSL::xml($definition->entry()->name(), $value);
            }

            if ($type === Entry\UuidEntry::class && (\is_string($value) || $value instanceof Entry\Type\Uuid)) {
                return EntryDSL::uuid($definition->entry()->name(), \is_string($value) ? $value : $value->toString());
            }

            if ($type === Entry\ObjectEntry::class && \is_object($value)) {
                return EntryDSL::object($definition->entry()->name(), $value);
            }

            if ($type === Entry\DateTimeEntry::class && $value instanceof \DateTimeInterface) {
                return EntryDSL::datetime($definition->entry()->name(), $value);
            }

            if ($type === Entry\DateTimeEntry::class && \is_string($value)) {
                return EntryDSL::datetime($definition->entry()->name(), new \DateTimeImmutable($value));
            }

            if ($type === Entry\ArrayEntry::class && \is_array($value)) {
                return EntryDSL::array($definition->entry()->name(), $value);
            }

            if ($type === Entry\EnumEntry::class && \is_string($value)) {
                /** @var class-string<\UnitEnum> $enumClass */
                $enumClass = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CLASS);
                /** @var array<\UnitEnum> $cases */
                $cases = $definition->metadata()->get(Schema\FlowMetadata::METADATA_ENUM_CASES);

                foreach ($cases as $case) {
                    if ($case->name === $value) {
                        return EntryDSL::enum($definition->entry()->name(), $case);
                    }
                }

                throw new InvalidArgumentException("Value \"not_valid\" can't be converted to " . $enumClass . ' enum');
            }

            if ($type === Entry\ListEntry::class && \is_array($value) && \array_is_list($value)) {
                try {
                    /** @var Type $listType */
                    $listType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

                    if (!\count($value)) {
                        return new Entry\ListEntry(
                            $definition->entry()->name(),
                            $listType,
                            []
                        );
                    }

                    if ($listType instanceof Entry\TypedCollection\ObjectType) {
                        /** @var mixed $firstValue */
                        $firstValue = \current($value);

                        if (\is_a($listType->class, \DateTimeInterface::class, true) && \is_string($firstValue)) {
                            return new Entry\ListEntry(
                                $definition->entry()->name(),
                                $listType,
                                \array_map(static fn (string $datetime) : \DateTimeImmutable => new \DateTimeImmutable($datetime), $value)
                            );
                        }
                    }

                    return new Entry\ListEntry(
                        $definition->entry()->name(),
                        $listType,
                        $value
                    );
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException("Field \"{$definition->entry()}\" conversion exception. {$e->getMessage()}", previous: $e);
                }
            }
        }

        throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\"");
    }

    /**
     * @return class-string
     */
    private function getClass(object $object) : string
    {
        $class = $object::class;

        if ($class === \DateTimeImmutable::class || $class === \DateTime::class) {
            return \DateTimeInterface::class;
        }

        return $class;
    }

    private function isJson(string $string) : bool
    {
        if ('{' !== $string[0] && '[' !== $string[0]) {
            return false;
        }

        if (
            (!\str_starts_with($string, '{') || !\str_ends_with($string, '}'))
            &&
            (!\str_starts_with($string, '[') || !\str_ends_with($string, ']'))
        ) {
            return false;
        }

        try {
            return \is_array(\json_decode($string, true, flags: \JSON_THROW_ON_ERROR));
        } catch (\Exception) {
            return false;
        }
    }

    private function isStructure(array $array) : bool
    {
        if (\array_is_list($array)) {
            return false;
        }

        if (!\count($array)) {
            return false;
        }

        foreach ($array as $key => $value) {
            if (!\is_string($key)) {
                return false;
            }

            if (\is_array($value) && !$this->isStructure($value)) {
                return false;
            }
        }

        return true;
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
