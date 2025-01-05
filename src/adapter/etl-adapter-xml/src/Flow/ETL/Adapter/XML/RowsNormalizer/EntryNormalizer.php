<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\RowsNormalizer;

use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{ListType, MapType, StructureType};
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{BooleanEntry, DateTimeEntry, EnumEntry, FloatEntry, IntegerEntry, JsonEntry, ListEntry, MapEntry, StringEntry, StructureEntry, UuidEntry};

final class EntryNormalizer
{
    public function __construct(
        private readonly PHPValueNormalizer $valueNormalizer,
    ) {

    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function normalize(Entry $entry) : XMLNode|XMLAttribute
    {
        if (\str_starts_with($entry->name(), $this->valueNormalizer->attributePrefix)) {
            return new XMLAttribute(\substr($entry->name(), \strlen($this->valueNormalizer->attributePrefix)), $entry->toString());
        }

        if ($entry instanceof ListEntry) {
            return $this->listToNode($entry);
        }

        if ($entry instanceof MapEntry) {
            return $this->mapToNode($entry);
        }

        if ($entry instanceof StructureEntry) {
            return $this->structureToNode($entry);
        }

        return match ($entry::class) {
            StringEntry::class => XMLNode::flatNode($entry->name(), $entry->value()),
            IntegerEntry::class => XMLNode::flatNode($entry->name(), (string) $entry->value()),
            FloatEntry::class => XMLNode::flatNode($entry->name(), (string) $entry->value()),
            BooleanEntry::class => XMLNode::flatNode($entry->name(), $entry->value() ? 'true' : 'false'),
            DateTimeEntry::class => XMLNode::flatNode($entry->name(), $entry->value()?->format($this->valueNormalizer->dateTimeFormat)),
            EnumEntry::class => XMLNode::flatNode($entry->name(), $entry->toString()),
            JsonEntry::class => XMLNode::flatNode($entry->name(), $entry->toString()),
            UuidEntry::class => XMLNode::flatNode($entry->name(), $entry->toString()),
            default => throw new InvalidArgumentException("Given entry type can't be converted to node, given entry type: " . $entry::class),
        };
    }

    /**
     * Since we don't have yet a good way of defining a custom metadata for a specific entries, we need to hardcode name of list node elements to "element".
     * It might be possible to use a schema here, if provided we might be able to take a metadata from entry definition and use it to define a node name.
     * However this might be a bit problematic in case of deeply nested lists.
     */
    private function listToNode(ListEntry $entry) : XMLNode
    {
        $node = XMLNode::nestedNode($entry->name());

        /** @var ListType $type */
        $type = $entry->type();

        $listValue = $entry->value();

        if (!\is_array($listValue) || !\count($listValue)) {
            return $node;
        }

        foreach ($listValue as $value) {
            $node = $node->append($this->valueNormalizer->normalize($this->valueNormalizer->listElementName, $type->element()->type(), $value));
        }

        return $node;
    }

    /**
     * There are at least 3 different ways of representing Maps in XML:
     *
     * Example 1:
     * <map>
     *    <element>
     *     <key></key>
     *     <value></value>
     *   </element>
     * </map>
     *
     * Example 2:
     * <map>
     *   <element key="xxx" value="xxx"></element>
     * </map>
     *
     * Example 3:
     * <map>
     *   <{key}>{value}</{key}>
     * </map>
     *
     * But we need to remember about node naming rules:
     *
     *   XML elements must follow these naming rules:
     *     - Names can contain letters, numbers, and other characters
     *     - Names cannot start with a number or punctuation character
     *     - Names cannot start with the letters xml (or XML, or Xml, etc)
     *     - Names cannot contain spaces
     *     - Any name can be used, no words are reserved.
     *
     * Because of that and because Map Values can be other nested structures, the only valid solution is solution from Example 1.
     */
    private function mapToNode(MapEntry $entry) : XMLNode
    {
        $node = XMLNode::nestedNode($entry->name());
        $mapValue = $entry->value();

        if (!\is_array($mapValue) || !\count($mapValue)) {
            return $node;
        }

        /** @var MapType $type */
        $type = $entry->type();

        foreach ($mapValue as $key => $value) {
            $node = $node->append($this->valueNormalizer->normalize($this->valueNormalizer->mapElementKeyName, $type->key()->type(), $key));
            $node = $node->append($this->valueNormalizer->normalize($this->valueNormalizer->mapElementValueName, $type->value()->type(), $value));
        }

        return $node;
    }

    private function structureToNode(StructureEntry $entry) : XMLNode
    {
        $node = XMLNode::nestedNode($entry->name());

        $value = $entry->value();

        if (!\is_array($value) || !\count($value)) {
            return $node;
        }

        /** @var StructureType $type */
        $type = $entry->type();

        $structureIterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
        $structureIterator->attachIterator(new \ArrayIterator($type->elements()), 'structure_element');
        $structureIterator->attachIterator(new \ArrayIterator($value), 'value_element');

        foreach ($structureIterator as $element) {
            /** @var StructureElement $structureElement */
            $structureElement = $element['structure_element'];
            $structureValue = $element['value_element'];

            $node = $node->append($this->valueNormalizer->normalize($structureElement->name(), $structureElement->type(), $structureValue));
        }

        return $node;
    }
}
