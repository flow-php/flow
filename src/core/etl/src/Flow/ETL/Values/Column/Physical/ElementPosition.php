<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Physical;

use function array_map;
use function array_reverse;
use function explode;
use function implode;
use function intval;
use function str_starts_with;
use function strlen;
use function strpos;
use function substr;

use const XML_ELEMENT_NODE;

/**
 * Format: "\x01<path>\0<markup length>\0<element><owner document>"; older builds wrote legacy forms.
 */
final readonly class ElementPosition
{
    /**
     * @param \DOMElement|\Dom\Element $element
     *
     * @return ?string null for an element outside its owner document's tree
     */
    public function path(object $element): ?string
    {
        $indices = [];
        $node = $element;

        while ($node->parentNode !== null && $node->parentNode->nodeType === XML_ELEMENT_NODE) {
            $index = 0;

            for (
                $sibling = $node->previousElementSibling;
                $sibling !== null;
                $sibling = $sibling->previousElementSibling
            ) {
                $index++;
            }

            $indices[] = $index;
            /** @var \DOMElement|\Dom\Element $node */
            $node = $node->parentNode;
        }

        return $node === $element->ownerDocument?->documentElement ? implode('/', array_reverse($indices)) : null;
    }

    public function physical(string $path, string $markup, string $document): string
    {
        return "\x01" . $path . "\0" . strlen($markup) . "\0" . $markup . $document;
    }

    /**
     * @return ?string null for bare element markup
     */
    public function pathOf(string $physical): ?string
    {
        return str_starts_with($physical, "\x01") || str_starts_with($physical, "\0")
            ? substr($physical, 1, (int) strpos($physical, "\0", 1) - 1)
            : null;
    }

    /**
     * @return ?string null for bytes that do not carry the element markup
     */
    public function markupOf(string $physical): ?string
    {
        if (!str_starts_with($physical, "\x01")) {
            return null;
        }

        $pathEnd = (int) strpos($physical, "\0", 1);
        $lengthEnd = (int) strpos($physical, "\0", $pathEnd + 1);

        return substr($physical, $lengthEnd + 1, (int) substr($physical, $pathEnd + 1, $lengthEnd - $pathEnd - 1));
    }

    public function documentOf(string $physical): string
    {
        $pathEnd = (int) strpos($physical, "\0", 1);

        if (!str_starts_with($physical, "\x01")) {
            return substr($physical, $pathEnd + 1);
        }

        $lengthEnd = (int) strpos($physical, "\0", $pathEnd + 1);

        return substr($physical, $lengthEnd + 1 + (int) substr($physical, $pathEnd + 1, $lengthEnd - $pathEnd - 1));
    }

    /**
     * @param null|\DOMElement|\Dom\Element $documentElement
     *
     * @return null|\DOMElement|\Dom\Element
     */
    public function element(?object $documentElement, string $path): ?object
    {
        $node = $documentElement;

        foreach ($path === '' ? [] : array_map(intval(...), explode('/', $path)) as $index) {
            $node = $node?->firstElementChild;

            for ($i = 0; $node !== null && $i < $index; $i++) {
                $node = $node->nextElementSibling;
            }

            if ($node === null) {
                return null;
            }
        }

        return $node;
    }
}
