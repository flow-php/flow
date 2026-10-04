<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_map;
use function count;
use function Flow\Types\DSL\type_bare;
use function implode;
use function str_repeat;
use function str_starts_with;
use function strlen;
use function substr;
use function substr_replace;

final class XMLEncoder
{
    private bool $rowElementChecked = false;

    private readonly XMLValueNodes $nodes;

    private readonly TextValues $text;

    public function __construct(
        private readonly ?XMLWriter $xmlWriter = null,
        private readonly string $attributePrefix = '_',
        string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        string $dateFormat = 'Y-m-d',
        string $listElementName = 'element',
        string $mapElementName = 'element',
        string $mapElementKeyName = 'key',
        string $mapElementValueName = 'value',
        private readonly string $rowElementName = 'row',
    ) {
        $this->text = new TextValues($dateTimeFormat, $dateFormat);
        $this->nodes = new XMLValueNodes(
            $this->text,
            $attributePrefix,
            $listElementName,
            $mapElementName,
            $mapElementKeyName,
            $mapElementValueName,
        );
    }

    public function encode(Rows $rows): string
    {
        $writer = $this->xmlWriter ?? throw new RuntimeException('XMLEncoder requires an XMLWriter to encode rows');
        $definitions = $rows->schema()->definitions();

        foreach ($definitions as $definition) {
            $this->nodes->refuseOptionalElements($definition->type());
        }

        if (!$this->rowElementChecked) {
            $writer->write(XMLNode::nestedNode($this->rowElementName));
            $this->rowElementChecked = true;
        }

        $count = $rows->count();

        if ($count < 1) {
            return '';
        }

        /** @var list<list<string>> $attributes */
        $attributes = [];
        /** @var list<list<string>> $children */
        $children = [];

        foreach ($definitions as $definition) {
            $name = $definition->entry()->name();
            $type = $definition->type();
            $bare = type_bare($type);
            $physicals = $rows->column($name)->physicals();

            if (str_starts_with($name, $this->attributePrefix)) {
                $attributes[] = $writer->attributes(
                    substr($name, strlen($this->attributePrefix)),
                    $this->text->texts($type, $physicals),
                );

                continue;
            }

            if ($bare instanceof ListType || $bare instanceof MapType || $bare instanceof StructureType) {
                $nodes = [];

                // @mago-ignore analysis:mixed-assignment
                foreach ($physicals as $physical) {
                    $nodes[] = $writer->write($this->nodes->of($name, $type, $physical));
                }

                $children[] = $nodes;

                continue;
            }

            $children[] = $writer->elements($name, $this->text->texts($type, $physicals));
        }

        if ($attributes === [] && $children === []) {
            return str_repeat('<' . $this->rowElementName . "/>\n", $count);
        }

        $pieces = [...$attributes, ...$children];
        $pieces[0] = substr_replace($pieces[0], '<' . $this->rowElementName . ($attributes === [] ? '>' : ''), 0, 0);

        if ($attributes !== [] && $children !== []) {
            $pieces[count($attributes) - 1] = substr_replace($pieces[count($attributes) - 1], '>', PHP_INT_MAX, 0);
        }

        $last = count($pieces) - 1;
        $pieces[$last] = substr_replace(
            $pieces[$last],
            $children === [] ? "/>\n" : '</' . $this->rowElementName . ">\n",
            PHP_INT_MAX,
            0,
        );

        if ($last === 0) {
            return implode('', $pieces[0]);
        }

        $lines = '';

        foreach (array_map(null, ...$pieces) as $row) {
            $lines .= implode('', $row);
        }

        return $lines;
    }
}
