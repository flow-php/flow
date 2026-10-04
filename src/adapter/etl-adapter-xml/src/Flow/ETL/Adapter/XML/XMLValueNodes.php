<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_string;
use function sprintf;
use function str_starts_with;
use function strlen;
use function substr;

final readonly class XMLValueNodes
{
    public function __construct(
        private TextValues $text,
        private string $attributePrefix = '_',
        private string $listElementName = 'element',
        private string $mapElementName = 'element',
        private string $mapElementKeyName = 'key',
        private string $mapElementValueName = 'value',
    ) {}

    /**
     * @param Type<mixed> $type
     */
    public function of(string $name, Type $type, mixed $physical): XMLNode
    {
        if ($physical === null) {
            return XMLNode::flatNode($name, '');
        }

        $bare = type_bare($type);

        if ($bare instanceof ListType) {
            /** @var list<mixed> $physical */
            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physical as $element) {
                $elements[] = $this->member($this->listElementName, $bare->element(), $element);
            }

            return $elements === [] ? XMLNode::nestedNode($name) : XMLNode::nested($name, ...$elements);
        }

        if ($bare instanceof MapType) {
            /** @var array<array-key, mixed> $physical */
            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physical as $key => $value) {
                $elements[] = XMLNode::nested(
                    $this->mapElementName,
                    str_starts_with($this->mapElementKeyName, $this->attributePrefix)
                        ? new XMLAttribute(
                            substr($this->mapElementKeyName, strlen($this->attributePrefix)),
                            (string) $key,
                        )
                        : XMLNode::flatNode($this->mapElementKeyName, (string) $key),
                    $this->member($this->mapElementValueName, $bare->value(), $value),
                );
            }

            return $elements === [] ? XMLNode::nestedNode($name) : XMLNode::nested($name, ...$elements);
        }

        if ($bare instanceof StructureType) {
            /** @var array<array-key, mixed> $physical */
            $elements = [];

            foreach ($bare->elements() as $element) {
                $elements[] = $this->member(
                    (string) $element->name,
                    $element->type,
                    array_key_exists($element->name, $physical) ? $physical[$element->name] : null,
                );
            }

            return XMLNode::nested($name, ...$elements);
        }

        return XMLNode::flatNode($name, $this->text->texts($type, [$physical])[0] ?? '');
    }

    /**
     * @param Type<mixed> $type
     */
    public function member(string $name, Type $type, mixed $physical): XMLAttribute|XMLNode
    {
        if (!str_starts_with($name, $this->attributePrefix)) {
            return $this->of($name, $type, $physical);
        }

        return new XMLAttribute(
            substr($name, strlen($this->attributePrefix)),
            type_string()->cast($this->text->texts($type, [$physical])[0] ?? null),
        );
    }

    /**
     * @param Type<mixed> $type
     */
    public function refuseOptionalElements(Type $type): void
    {
        $bare = type_bare($type);

        if ($bare instanceof ListType) {
            $this->refuseOptionalElements($bare->element());
        }

        if ($bare instanceof MapType) {
            $this->refuseOptionalElements($bare->value());
        }

        if ($bare instanceof StructureType) {
            foreach ($bare->elements() as $element) {
                if ($element->optional) {
                    throw new RuntimeException(sprintf(
                        'XML encoder does not support structure optional elements, given: %s',
                        $bare->toString(),
                    ));
                }

                $this->refuseOptionalElements($element->type);
            }
        }
    }
}
