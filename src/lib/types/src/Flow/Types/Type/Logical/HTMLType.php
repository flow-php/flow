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
    public const string HTML_ALIKE_REGEX = <<<'REGXP'
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
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value) && \class_exists('\Dom\HTMLDocument') && \preg_match(self::HTML_ALIKE_REGEX, $value) === 1) {
            return HTMLDocument::createFromString($value, \LIBXML_NOERROR);
        }

        throw new CastingException($value, $this);
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
