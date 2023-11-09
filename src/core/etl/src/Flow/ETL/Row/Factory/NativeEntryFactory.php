<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use Flow\ETL\DSL\Entry as EntryDSL;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\TypeFactory;
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

        if (\is_string($value)) {
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

        try {
            $valueType = (new TypeFactory())->getType($value);

            if ($valueType instanceof ArrayType || $valueType instanceof MapType) {
                return new Row\Entry\ArrayEntry($entryName, $value);
            }

            if ($valueType instanceof StructureType) {
                return new Row\Entry\StructureEntry($entryName, $value, $valueType);
            }

            if ($valueType instanceof ListType) {
                return new Entry\ListEntry($entryName, $valueType->element(), $value);
            }
        } catch (InvalidArgumentException $exception) {
            // Used below
        }

        $type = \gettype($value);

        throw new InvalidArgumentException("{$type} can't be converted to any known Entry", 0, $exception ?? null);
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
                return EntryDSL::json($definition->entry()->name(), $value);
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

                throw new InvalidArgumentException("Value \"{$value}\" can't be converted to " . $enumClass . ' enum');
            }

            if ($type === Entry\JsonEntry::class && \is_array($value)) {
                try {
                    return EntryDSL::json_object($definition->entry()->name(), $value);
                } catch (InvalidArgumentException $e) {
                    return EntryDSL::json($definition->entry()->name(), $value);
                }
            }

            if ($type === Entry\ArrayEntry::class && \is_array($value)) {
                return EntryDSL::array($definition->entry()->name(), $value);
            }

            if ($type === Entry\ListEntry::class && \is_array($value) && \array_is_list($value)) {
                try {
                    /** @var ListElement $listType */
                    $listType = $definition->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

                    if (!\count($value)) {
                        return new Entry\ListEntry(
                            $definition->entry()->name(),
                            $listType,
                            []
                        );
                    }

                    if ($listType->value() instanceof ObjectType) {
                        /** @var mixed $firstValue */
                        $firstValue = \current($value);

                        if (\is_a($listType->value()->class, \DateTimeInterface::class, true) && \is_string($firstValue)) {
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
