<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Physical;

use function array_key_exists;
use function hexdec;
use function html_entity_decode;
use function intval;
use function preg_replace_callback;
use function str_contains;
use function str_starts_with;
use function substr;

/**
 * libxml2 < 2.13 writes non-ASCII as char refs; decoding them gives one markup on every version.
 */
final readonly class XmlCharacterReferences
{
    public function decoded(string $xml): string
    {
        if (!str_contains($xml, '&#')) {
            return $xml;
        }

        return (string) preg_replace_callback(
            '/<!\[CDATA\[.*?\]\]>|<!--.*?-->|&#(x[0-9A-Fa-f]+|[0-9]+);/s',
            static function (array $match): string {
                if (!array_key_exists(1, $match)) {
                    return $match[0];
                }

                return (str_starts_with($match[1], 'x') ? hexdec(substr($match[1], 1)) : intval($match[1])) < 0x80
                    ? $match[0]
                    : html_entity_decode($match[0], ENT_XML1 | ENT_QUOTES, 'UTF-8');
            },
            $xml,
        );
    }
}
