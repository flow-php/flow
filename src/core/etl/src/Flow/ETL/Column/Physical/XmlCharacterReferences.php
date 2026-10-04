<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use function array_key_exists;
use function hexdec;
use function html_entity_decode;
use function intval;
use function preg_replace_callback;
use function str_contains;
use function str_starts_with;
use function substr;

/**
 * libxml2 before 2.13 serializes non-ASCII attribute characters as character references (`&#x17C;`), later versions
 * write them as UTF-8. Decoding the references of non-ASCII characters outside CDATA sections and comments gives the
 * same markup on every libxml2; references below U+0080 stay, they may escape markup.
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

                $codepoint = str_starts_with($match[1], 'x') ? hexdec(substr($match[1], 1)) : intval($match[1]);

                return $codepoint < 0x80 ? $match[0] : html_entity_decode($match[0], ENT_XML1 | ENT_QUOTES, 'UTF-8');
            },
            $xml,
        );
    }
}
