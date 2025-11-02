<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;

final readonly class HTMLDocument implements \Stringable
{
    private const HTML_ALIKE_REGEX = <<<'REGXP'
@^
    <!DOCTYPE\s+html[^>]*>\s*      # must start with <!DOCTYPE html ...>
    <html[^>]*>\s*                 # opening <html>
    <head[^>]*>.*?<\/head>\s*      # exactly one <head> ... </head>
    <body[^>]*>.*?<\/body>\s*      # exactly one <body> ... </body>
    <\/html>\s*                    # closing </html>
$@mix
REGXP;

    private string $value;

    public function __construct(string|object $value)
    {
        if ($value instanceof \DOMDocument) {
            $value = $value->saveHTML($value) ?: '';
        } elseif (is_a($value, '\Dom\HTMLDocument', true)) {
            /* @phpstan-ignore-next-line */
            $value = $value->saveHtml();
        } elseif (!is_string($value)) {
            throw new InvalidArgumentException('Invalid HTML document type: ' . $value::class);
        }

        // Cut all new lines and tabs
        $value = trim(str_replace(["\n", "\t"], '', $value));

        if (!$this->isValid($value)) {
            throw new InvalidArgumentException('Invalid HTML document given: ' . var_export($value, true));
        }

        $this->value = $value;
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

    private function isValid(string $value) : bool
    {
        if ('' === $value) {
            return false;
        }

        return \preg_match(self::HTML_ALIKE_REGEX, $value) === 1;
    }
}
