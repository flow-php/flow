<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use function array_fill;
use function array_map;
use function array_merge;
use function array_sum;
use function count;
use function explode;
use function implode;
use function intdiv;
use function max;
use function mb_str_split;
use function mb_strlen;
use function rtrim;
use function str_repeat;

/**
 * Boxes on a grid: a node sits above its first child, later children to the right, joined by a line from the
 * node's right edge.
 */
final readonly class BoxLayout implements Layout
{
    private const int WIDTH = 29;

    private const int TEXT = 25;

    public function __construct(
        private Details $details = new Details(),
    ) {}

    public function render(Entry $root): string
    {
        /** @var list<array{entry: Entry, column: int, depth: int, parent: ?int}> $boxes */
        $boxes = [];
        $this->place($root, 0, 0, null, $boxes);

        /** @var array<int, int> $heights */
        $heights = [];

        foreach ($boxes as $box) {
            $heights[$box['depth']] = max($heights[$box['depth']] ?? 0, count($this->content($box['entry'])) + 2);
        }

        /** @var array<int, int> $tops */
        $tops = [];
        $top = 0;

        for ($depth = 0; $depth < count($heights); $depth++) {
            $tops[$depth] = $top;
            $top += $heights[$depth];
        }

        $canvas = array_fill(0, max(0, $top), array_fill(0, max(0, $this->span($root) * self::WIDTH), ' '));

        foreach ($boxes as $box) {
            $this->box(
                $canvas,
                $box['entry'],
                $box['column'] * self::WIDTH,
                $tops[$box['depth']],
                $heights[$box['depth']],
                $box['parent'] !== null,
            );
        }

        foreach ($boxes as $box) {
            $this->links($canvas, $box, $boxes, $tops[$box['depth']], $heights[$box['depth']]);
        }

        return implode("\n", array_map(static fn(array $row): string => rtrim(implode('', $row)), $canvas));
    }

    /**
     * @param list<array{entry: Entry, column: int, depth: int, parent: ?int}> $boxes
     */
    public function place(Entry $entry, int $column, int $depth, ?int $parent, array &$boxes): void
    {
        $boxes[] = ['entry' => $entry, 'column' => $column, 'depth' => $depth, 'parent' => $parent];
        $index = count($boxes) - 1;

        foreach ($entry->children as $child) {
            $this->place($child, $column, $depth + 1, $index, $boxes);
            $column += $this->span($child);
        }
    }

    public function span(Entry $entry): int
    {
        return $entry->children === [] ? 1 : array_sum(array_map($this->span(...), $entry->children));
    }

    /**
     * @return list<string>
     */
    public function content(Entry $entry): array
    {
        $title = $entry->title($this->details->name($entry->node));

        if ($entry->shared) {
            return [...$this->wrap($title), '(shared)'];
        }

        $details = $this->details->lines($entry->node);

        if ($details === []) {
            return $this->wrap($title);
        }

        return array_merge($this->wrap($title), [str_repeat('─', 20)], ...array_map($this->wrap(...), $details));
    }

    /**
     * @return list<string>
     */
    public function wrap(string $text): array
    {
        $lines = [];
        $line = '';

        foreach (explode(' ', $text) as $word) {
            foreach (mb_str_split($word, self::TEXT) as $piece) {
                if ($line !== '' && (mb_strlen($line) + 1 + mb_strlen($piece)) > self::TEXT) {
                    $lines[] = $line;
                    $line = '';
                }

                $line = $line === '' ? $piece : $line . ' ' . $piece;
            }
        }

        $lines[] = $line;

        return $lines;
    }

    /**
     * @param list<list<string>> $canvas
     */
    public function box(array &$canvas, Entry $entry, int $x, int $y, int $height, bool $hasParent): void
    {
        $inner = self::WIDTH - 2;
        $center = $x + intdiv(self::WIDTH, 2);

        $this->write($canvas, $x, $y, '┌' . str_repeat('─', $inner) . '┐');
        $this->write($canvas, $x, $y + $height - 1, '└' . str_repeat('─', $inner) . '┘');

        $content = $this->content($entry);

        for ($row = 1; $row < ($height - 1); $row++) {
            $text = $content[$row - 1] ?? '';
            $left = max(0, intdiv($inner - mb_strlen($text), 2));
            $this->write(
                $canvas,
                $x,
                $y + $row,
                '│' . str_repeat(' ', $left) . $text . str_repeat(' ', max(0, $inner - $left - mb_strlen($text))) . '│',
            );
        }

        if ($hasParent) {
            $canvas[$y][$center] = '┴';
        }

        if ($entry->children !== []) {
            $canvas[$y + $height - 1][$center] = '┬';
        }
    }

    /**
     * Draws the line from a box's right edge to each child after the first; the first child sits right below it.
     *
     * @param list<list<string>> $canvas
     * @param array{entry: Entry, column: int, depth: int, parent: ?int} $box
     * @param list<array{entry: Entry, column: int, depth: int, parent: ?int}> $boxes
     */
    public function links(array &$canvas, array $box, array $boxes, int $top, int $height): void
    {
        $children = $box['entry']->children;

        if (count($children) < 2) {
            return;
        }

        $row = $top + intdiv($height, 2);
        $edge = (($box['column'] + 1) * self::WIDTH) - 1;
        $canvas[$row][$edge] = '├';
        $column = $box['column'];
        $centers = [];

        foreach ($children as $index => $child) {
            if ($index > 0) {
                $centers[] = ($column * self::WIDTH) + intdiv(self::WIDTH, 2);
            }

            $column += $this->span($child);
        }

        $last = $centers[count($centers) - 1];

        for ($x = $edge + 1; $x < $last; $x++) {
            $canvas[$row][$x] = '─';
        }

        foreach ($centers as $center) {
            $canvas[$row][$center] = $center === $last ? '┐' : '┬';

            for ($y = $row + 1; $y < ($top + $height); $y++) {
                $canvas[$y][$center] = '│';
            }
        }
    }

    /**
     * @param list<list<string>> $canvas
     */
    public function write(array &$canvas, int $x, int $y, string $text): void
    {
        foreach (mb_str_split($text) as $offset => $character) {
            $canvas[$y][$x + $offset] = $character;
        }
    }
}
