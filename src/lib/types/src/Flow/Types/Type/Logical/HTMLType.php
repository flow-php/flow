<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Dom\HTMLDocument;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<HTMLDocument>
 */
final readonly class HTMLType implements Type
{
    public const HTML_ALIKE_REGEX = <<<'REGXP'
@^
    <!DOCTYPE\s+html[^>]*>\s*      # must start with <!DOCTYPE html ...>
    <html[^>]*>\s*                 # opening <html>
    <head[^>]*>.*?<\/head>\s*      # exactly one <head> ... </head>
    <body[^>]*>.*?<\/body>\s*      # exactly one <body> ... </body>
    <\/html>\s*                    # closing </html>
$@isx
REGXP;

    public function assert(mixed $value) : HTMLDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : HTMLDocument
    {
        if (!$this->isValid($value)) {
            throw new CastingException($value, $this);
        }

        /* @phpstan-ignore-next-line */
        if (\is_string($value)) {
            return HTMLDocument::createFromString($value, \LIBXML_NOERROR);
        }

        return $value;
    }

    public function isValid(mixed $value) : bool
    {
        // \Dom\HTMLDocument exist in PHP 8.4+
        if (!\class_exists('\Dom\HTMLDocument')) {
            return false;
        }

        return $value instanceof HTMLDocument;
    }

    public function normalize() : array
    {
        return [
            'type' => 'html',
        ];
    }

    public function toString() : string
    {
        return 'html';
    }
}
