<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use function array_key_last;
use function implode;

final readonly class TreeLayout implements Layout
{
    public function render(Entry $root): string
    {
        return implode("\n", $this->lines($root, '', ''));
    }

    /**
     * @param string $connector drawn before this entry's line
     * @param string $indent drawn before every line under this entry
     *
     * @return list<string>
     */
    public function lines(Entry $entry, string $connector, string $indent): array
    {
        if ($entry->shared) {
            return [$connector . $entry->title() . ' (shared)'];
        }

        $title = $connector . $entry->title();
        $lines = [$entry->suffix === '' ? $title : $title . '  ' . $entry->suffix];
        // the children's connector runs through the details, so the details sit inside the node's branch
        $rail = $entry->children === [] ? '   ' : '│  ';

        foreach ($entry->lines as $detail) {
            $lines[] = $indent . $rail . $detail;
        }

        foreach ($this->branches($entry, $indent) as [$child, $childConnector, $childIndent]) {
            foreach ($this->lines($child, $childConnector, $childIndent) as $line) {
                $lines[] = $line;
            }
        }

        return $lines;
    }

    /**
     * Each child of $entry with the connector drawn before its line and the indent drawn before its children's lines.
     *
     * @return list<array{Entry, string, string}>
     */
    public function branches(Entry $entry, string $indent): array
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
