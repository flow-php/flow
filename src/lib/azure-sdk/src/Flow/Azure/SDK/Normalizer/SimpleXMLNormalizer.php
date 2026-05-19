<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Normalizer;

use Flow\Azure\SDK\Exception\Exception;
use Flow\Azure\SDK\Normalizer;
use SimpleXMLElement;

use function class_exists;

final class SimpleXMLNormalizer implements Normalizer
{
    public function __construct()
    {
        if (!class_exists('SimpleXMLElement')) {
            throw new Exception('SimpleXML extension is required to use this normalizer');
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(string $data): array
    {
        return $this->normalize(new SimpleXMLElement($data));
    }

    /**
     * @return array<string, mixed>
     */
    private function normalize(SimpleXMLElement $xml): array
    {
        $children = $xml->children();

        if ($children === null) {
            return [];
        }

        /** @var array<string, list<\SimpleXMLElement>> $grouped */
        $grouped = [];

        foreach ($children as $name => $child) {
            if ($child === null) {
                continue;
            }

            $grouped[$name][] = $child;
        }

        $normalized = [];

        foreach ($grouped as $name => $group) {
            if (count($group) === 1) {
                $normalized[$name] = $this->valueOf($group[0]);

                continue;
            }

            $values = [];

            foreach ($group as $child) {
                $values[] = $this->valueOf($child);
            }

            $normalized[$name] = $values;
        }

        return $normalized;
    }

    /**
     * @return null|array<string, mixed>|string
     */
    private function valueOf(SimpleXMLElement $element): array|string|null
    {
        if ($element->count() > 0) {
            $value = $this->normalize($element);

            return count($value) === 0 ? null : $value;
        }

        $text = (string) $element;

        return $text === '' ? null : $text;
    }
}
