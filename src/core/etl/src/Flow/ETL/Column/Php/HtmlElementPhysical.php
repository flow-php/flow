<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Dom\HTMLDocument;
use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function class_exists;
use function is_object;
use function is_string;
use function sprintf;

use const LIBXML_NOERROR;

final readonly class HtmlElementPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert(is_object($value));

        /** @var \Dom\HTMLElement $value */
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $html = $value->ownerDocument?->saveHtml($value);

        if (!is_string($html)) {
            throw new InvalidArgumentException('Floe failed to convert HTMLElement to HTML string');
        }

        return $html;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        if (!class_exists('\Dom\HTMLDocument')) {
            throw new InvalidArgumentException('Floe cannot restore HTML values, \Dom\HTMLDocument requires PHP 8.4+');
        }

        // @mago-expect analysis:unavailable-method
        $element = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><body>' . $physical . '</body></html>',
            LIBXML_NOERROR,
        )->body?->firstElementChild;

        if ($element === null) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore HTMLElement from "%s"', $physical));
        }

        return $element;
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $values[] = $physical === null ? null : $this->fromPhysical($physical);
        }

        return $values;
    }
}
