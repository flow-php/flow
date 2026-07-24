<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use function explode;
use function preg_match_all;
use function preg_split;
use function strpos;
use function strtolower;
use function substr;
use function trim;

/**
 * Minimal RFC 8288 `Link` header parser. Malformed input yields no match (never throws).
 */
final class LinkHeaderParser
{
    /**
     * @param array<string> $linkHeaders
     */
    public function next(array $linkHeaders, string $rel): ?string
    {
        foreach ($linkHeaders as $header) {
            $matches = [];

            if (preg_match_all('/<([^>]*)>\s*;\s*([^,]*)/', $header, $matches, PREG_SET_ORDER) === false) {
                continue;
            }

            foreach ($matches as $match) {
                if ($this->relMatches($match[2], $rel)) {
                    return trim($match[1]);
                }
            }
        }

        return null;
    }

    private function relMatches(string $params, string $rel): bool
    {
        foreach (explode(';', $params) as $param) {
            $equals = strpos($param, '=');

            if ($equals === false) {
                continue;
            }

            if (strtolower(trim(substr($param, 0, $equals))) !== 'rel') {
                continue;
            }

            foreach (preg_split('/\s+/', trim(trim(substr($param, $equals + 1)), '"\'')) ?: [] as $relType) {
                if ($relType === $rel) {
                    return true;
                }
            }
        }

        return false;
    }
}
