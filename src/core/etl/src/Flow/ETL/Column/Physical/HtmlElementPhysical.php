<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Dom\HTMLDocument;
use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function class_exists;
use function is_object;
use function is_string;
use function sprintf;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final readonly class HtmlElementPhysical implements Physical
{
    public function __construct(
        private ElementPosition $position = new ElementPosition(),
    ) {}

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

        $path = $this->position->path($value);

        if ($path === null) {
            return $html;
        }

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $document = $value->ownerDocument?->saveHtml();
        assert(is_string($document));

        return $this->position->physical($path, $html, $document);
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        if (!class_exists('\Dom\HTMLDocument')) {
            throw new InvalidArgumentException('Floe cannot restore HTML values, \Dom\HTMLDocument requires PHP 8.4+');
        }

        $path = $this->position->pathOf($physical);

        // @mago-expect analysis:unavailable-method
        $element = $path === null
            ? HTMLDocument::createFromString(
                '<!DOCTYPE html><html><body>' . $physical . '</body></html>',
                LIBXML_NOERROR,
            )->body?->firstElementChild
            : $this->position->element(
                // @mago-expect analysis:unavailable-method
                HTMLDocument::createFromString(
                    $this->position->documentOf($physical),
                    LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR,
                )->documentElement,
                $path,
            );

        if ($element === null) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore HTMLElement from "%s"', $physical));
        }

        return $element;
    }

    public function markup(string $physical): string
    {
        $markup = $this->position->markupOf($physical);

        if ($markup !== null) {
            return $markup;
        }

        if ($this->position->pathOf($physical) === null) {
            return $physical;
        }

        /** @var \Dom\HTMLElement $element */
        $element = $this->fromPhysical($physical);
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $html = $element->ownerDocument?->saveHtml($element);
        assert(is_string($html));

        return $html;
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
