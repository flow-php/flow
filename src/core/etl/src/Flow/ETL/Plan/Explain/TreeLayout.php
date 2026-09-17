<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use function implode;

final readonly class TreeLayout implements Layout
{
    /**
     * @param bool $declarations the declarations optimizer rules read on every node's line, in place of the notes
     *                           that say the same in plain words
     */
    public function __construct(
        private Details $details = new Details(),
        private bool $declarations = false,
    ) {}

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
            return [$connector . $entry->title($this->details->name($entry->node)) . ' (shared)'];
        }

        $title = $connector . $entry->title($this->details->name($entry->node));
        $lines = [$this->declarations ? $title . '  ' . $this->details->declarations($entry->node) : $title];
        // the children's connector runs through the details, so the details sit inside the node's branch
        $rail = $entry->children === [] ? '   ' : '│  ';
        $details = $this->declarations ? $this->details->labelled($entry->node) : $this->details->lines($entry->node);

        foreach ($details as $detail) {
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
