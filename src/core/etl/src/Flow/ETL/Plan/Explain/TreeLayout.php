<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

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

        foreach ((new Branches())->of($entry, $indent) as [$child, $childConnector, $childIndent]) {
            foreach ($this->lines($child, $childConnector, $childIndent) as $line) {
                $lines[] = $line;
            }
        }

        return $lines;
    }
}
