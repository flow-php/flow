<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use function array_key_last;

final readonly class Branches
{
    /**
     * Each child of $entry with the connector drawn before its line and the indent drawn before its children's lines.
     *
     * @return list<array{Entry, string, string}>
     */
    public function of(Entry $entry, string $indent): array
    {
        $last = array_key_last($entry->children);
        $branches = [];

        foreach ($entry->children as $index => $child) {
            $branches[] = $index === $last
                ? [$child, $indent . '└─ ', $indent . '   ']
                : [$child, $indent . '├─ ', $indent . '│  '];
        }

        return $branches;
    }
}
