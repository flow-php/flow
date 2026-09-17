<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use ReflectionClass;

use function implode;
use function sprintf;

final readonly class Details
{
    public function name(object $object): string
    {
        return (new ReflectionClass($object))->getShortName();
    }

    /**
     * What the node does, one "Label: value" per line, followed by what it means for the rows in plain words.
     *
     * @return list<string>
     */
    public function lines(Node $node): array
    {
        return [...$this->labelled($node), ...$this->notes($node)];
    }

    /**
     * @return list<string>
     */
    public function labelled(Node $node): array
    {
        return match (true) {
            $node instanceof Read => $this->read($node),
            $node instanceof Result => ['Rows this plan hands out: to the trigger, or to the node reading it'],
            $node instanceof Filter => ['Condition: ' . $this->name($node->function)],
            $node instanceof Until => ['Until: ' . $this->name($node->function)],
            $node instanceof WithColumn => [sprintf('Column: %s = %s', $node->name(), $this->name($node->function))],
            $node instanceof Write => ['Loader: ' . $this->name($node->loader)],
            $node instanceof Limit => ['Limit: ' . $node->limit],
            $node instanceof Offset => ['Skip: ' . $node->offset],
            $node instanceof TopN => ['Top: ' . $node->limit],
            $node instanceof Rename => [sprintf('Rename: %s → %s', $node->from, $node->to)],
            default => [],
        };
    }

    /**
     * @return list<string>
     */
    public function read(Read $read): array
    {
        $lines = ['Extractor: ' . $this->name($read->extractor())];
        $limit = $read->limit();

        if ($limit !== null) {
            $lines[] = 'Limit: ' . $limit;
        }

        // every file source reads only files unless a filter was pushed into it
        if (!$read->pathFilter() instanceof OnlyFiles) {
            $lines[] = 'Files: ' . $this->name($read->pathFilter());
        }

        return $lines;
    }

    /**
     * The declarations below in plain words, for the ones that change how the rows flow.
     *
     * @return list<string>
     */
    public function notes(Node $node): array
    {
        $notes = [];
        $redefines = $node->redefines();

        if ($node->materialization() === Materialization::blocking) {
            $notes[] = 'Buffers all rows before passing them on';
        }

        if ($redefines->unknown) {
            $notes[] = 'Defines columns known only at run time';
        } elseif ($redefines->names !== []) {
            $notes[] = 'Defines columns: ' . implode(', ', $redefines->names);
        }

        return $notes;
    }

    /**
     * The declarations optimizer rules read, on one line.
     */
    public function declarations(Node $node): string
    {
        $redefines = $node->redefines();
        $declarations = [
            $node->rowCount()->name,
            $node->transparency()->name,
            $node->materialization()->name,
        ];

        if ($redefines->unknown) {
            $declarations[] = 'redefines unknown';
        } elseif ($redefines->names !== []) {
            $declarations[] = 'redefines ' . implode(', ', $redefines->names);
        }

        return implode(' · ', $declarations);
    }
}
