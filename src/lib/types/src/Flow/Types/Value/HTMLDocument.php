<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;

final class HTMLDocument implements \Stringable
{
    private string $value;

    public function __construct(string|object $value)
    {
        if (\is_string($value)) {
            if (\class_exists('\Dom\HTMLDocument', false)) {
                $options = \LIBXML_HTML_NOIMPLIED;

                if (defined('Dom\HTML_NO_DEFAULT_NS')) {
                    $options |= constant('\Dom\HTML_NO_DEFAULT_NS');
                }

                $document = \Dom\HTMLDocument::createFromString($value, $options);

                $this->value = $document->saveHTML();
            } else {
                $document = new \DOMDocument();

                $result = @$document->loadHTML($value, \LIBXML_HTML_NOIMPLIED | \LIBXML_HTML_NODEFDTD);

                if ($result === false) {
                    throw new InvalidArgumentException("Invalid value '{$value}'");
                }

                $value = $document->saveHTML() ?: throw new InvalidArgumentException("Invalid value '{$value}'");

                $this->value = trim($value);
            }
        } elseif ($value instanceof \DOMDocument) {
            $value = $value->saveHTML($value->documentElement) ?: throw new InvalidArgumentException('Invalid value ' . var_export($value, true));

            $this->value = trim($value);
        } elseif (is_a($value, '\Dom\HTMLDocument', true)) {
            /* @phpstan-ignore-next-line */
            $this->value = $value->saveHtml();
        } else {
            throw new InvalidArgumentException('Invalid value ' . var_export($value, true));
        }
    }

    public static function fromString(string $value) : self
    {
        return new self($value);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function isEqual(self $type) : bool
    {
        return $this->toString() === $type->toString();
    }

    public function toString() : string
    {
        return $this->value;
    }
}
