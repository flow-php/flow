<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\XMLWriter;

use DOMDocument;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\Types\Exception\CastingException;

use function array_key_exists;
use function array_keys;
use function Flow\Types\DSL\type_string;
use function in_array;
use function preg_grep;
use function str_contains;
use function strstr;
use function strtr;
use function substr_replace;

/**
 * DOMDocumentWriter's output on libxml 2.14 and later, byte for byte, without building a DOM for every row: libxml's
 * escaping, a control character written as U+FFFD, a value cut at its first NUL, and a name DOM refuses refused with
 * DOM's exception.
 */
final class StringXMLWriter implements XMLWriter
{
    /**
     * Everything libxml escapes in an attribute value, plus the control characters it replaces.
     */
    private const array ATTRIBUTE = [
        '&' => '&amp;',
        '<' => '&lt;',
        '>' => '&gt;',
        '"' => '&quot;',
        "\t" => '&#9;',
        "\n" => '&#10;',
        "\r" => '&#13;',
        "\x01" => '&#xFFFD;',
        "\x02" => '&#xFFFD;',
        "\x03" => '&#xFFFD;',
        "\x04" => '&#xFFFD;',
        "\x05" => '&#xFFFD;',
        "\x06" => '&#xFFFD;',
        "\x07" => '&#xFFFD;',
        "\x08" => '&#xFFFD;',
        "\x0B" => '&#xFFFD;',
        "\x0C" => '&#xFFFD;',
        "\x0E" => '&#xFFFD;',
        "\x0F" => '&#xFFFD;',
        "\x10" => '&#xFFFD;',
        "\x11" => '&#xFFFD;',
        "\x12" => '&#xFFFD;',
        "\x13" => '&#xFFFD;',
        "\x14" => '&#xFFFD;',
        "\x15" => '&#xFFFD;',
        "\x16" => '&#xFFFD;',
        "\x17" => '&#xFFFD;',
        "\x18" => '&#xFFFD;',
        "\x19" => '&#xFFFD;',
        "\x1A" => '&#xFFFD;',
        "\x1B" => '&#xFFFD;',
        "\x1C" => '&#xFFFD;',
        "\x1D" => '&#xFFFD;',
        "\x1E" => '&#xFFFD;',
        "\x1F" => '&#xFFFD;',
    ];

    /**
     * Everything libxml escapes in text, plus the control characters it replaces - quotes, tabs and line feeds
     * stay as they are.
     */
    private const array TEXT = [
        '&' => '&amp;',
        '<' => '&lt;',
        '>' => '&gt;',
        "\r" => '&#13;',
        "\x01" => '&#xFFFD;',
        "\x02" => '&#xFFFD;',
        "\x03" => '&#xFFFD;',
        "\x04" => '&#xFFFD;',
        "\x05" => '&#xFFFD;',
        "\x06" => '&#xFFFD;',
        "\x07" => '&#xFFFD;',
        "\x08" => '&#xFFFD;',
        "\x0B" => '&#xFFFD;',
        "\x0C" => '&#xFFFD;',
        "\x0E" => '&#xFFFD;',
        "\x0F" => '&#xFFFD;',
        "\x10" => '&#xFFFD;',
        "\x11" => '&#xFFFD;',
        "\x12" => '&#xFFFD;',
        "\x13" => '&#xFFFD;',
        "\x14" => '&#xFFFD;',
        "\x15" => '&#xFFFD;',
        "\x16" => '&#xFFFD;',
        "\x17" => '&#xFFFD;',
        "\x18" => '&#xFFFD;',
        "\x19" => '&#xFFFD;',
        "\x1A" => '&#xFFFD;',
        "\x1B" => '&#xFFFD;',
        "\x1C" => '&#xFFFD;',
        "\x1D" => '&#xFFFD;',
        "\x1E" => '&#xFFFD;',
        "\x1F" => '&#xFFFD;',
    ];

    /**
     * Element and attribute names DOM already accepted - a row repeats the same few names, so each is checked once.
     *
     * @var array<string, true>
     */
    private array $names = [];

    private ?DOMDocument $validator = null;

    public function attributes(string $name, array $values): array
    {
        $name = $this->attributeName($name);

        if (in_array(null, $values, true)) {
            throw new CastingException(null, type_string());
        }

        /** @var list<string> */
        return substr_replace(
            substr_replace($this->escapeAll($values, self::ATTRIBUTE, '/[&<>"\x00-\x1F]/'), ' ' . $name . '="', 0, 0),
            '"',
            PHP_INT_MAX,
            0,
        );
    }

    public function elements(string $name, array $values): array
    {
        $name = $this->elementName($name);

        /** @var list<string> */
        return substr_replace(
            substr_replace($this->escapeAll($values, self::TEXT, '/[&<>\x00-\x08\x0B-\x1F]/'), '<' . $name . '>', 0, 0),
            '</' . $name . '>',
            PHP_INT_MAX,
            0,
        );
    }

    public function write(XMLNode $node): string
    {
        $name = $this->elementName($node->name);
        $attributes = [];

        // DOM keeps an attribute set twice at its first position, with its last value
        foreach ($node->attributes as $attribute) {
            $attributes[$this->attributeName($attribute->name)] = $attribute->value;
        }

        $xml = '<' . $name;

        foreach ($attributes as $attributeName => $value) {
            $xml .= ' ' . $attributeName . '="' . self::escape($value, self::ATTRIBUTE) . '"';
        }

        if ($node->value !== null) {
            return $xml . '>' . self::escape($node->value, self::TEXT) . '</' . $name . '>';
        }

        if ($node->children === []) {
            return $xml . '/>';
        }

        $xml .= '>';

        foreach ($node->children as $child) {
            $xml .= $this->write($child);
        }

        return $xml . '</' . $name . '>';
    }

    /**
     * @param array<string, string> $map
     */
    private static function escape(string $value, array $map): string
    {
        // libxml reads a value as a C string, so it ends at the first NUL
        if (str_contains($value, "\0")) {
            $value = (string) strstr($value, "\0", true);
        }

        return strtr($value, $map);
    }

    /**
     * @param list<?string> $values null is ''
     * @param array<string, string> $map
     *
     * @return list<string>
     */
    private function escapeAll(array $values, array $map, string $escaped): array
    {
        foreach (array_keys($values, null, true) as $i) {
            $values[$i] = '';
        }

        /** @var list<string> $texts */
        $texts = $values;

        foreach (preg_grep($escaped, $texts) ?: [] as $i => $text) {
            $texts[$i] = self::escape($text, $map);
        }

        return $texts;
    }

    private function attributeName(string $name): string
    {
        if (!array_key_exists($name, $this->names)) {
            // libxml checks the name as DOMDocumentWriter's setAttribute() does, and throws the same DOMException
            ($this->validator ??= new DOMDocument())->createAttribute($name);
            $this->names[$name] = true;
        }

        return $name;
    }

    private function elementName(string $name): string
    {
        if (!array_key_exists($name, $this->names)) {
            // throws DOMException exactly where DOMDocumentWriter's createElement() would
            ($this->validator ??= new DOMDocument())->createElement($name);
            $this->names[$name] = true;
        }

        return $name;
    }
}
