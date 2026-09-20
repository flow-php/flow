<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Aggregate;
use Flow\ETL\Plan\Node\Batch;
use Flow\ETL\Plan\Node\Cache;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Distinct;
use Flow\ETL\Plan\Node\Drop;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Join;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\Repartition;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\Validate;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Row\Reference;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use ReflectionClass;

use function array_map;
use function implode;
use function sprintf;
use function str_ends_with;
use function substr;

final readonly class Details
{
    public function __construct(
        private Condition $condition = new Condition(),
    ) {}

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
            $node instanceof Join => $this->join($node),
            $node instanceof CrossJoin => $this->crossJoin($node),
            $node instanceof Result => ['Rows this plan hands out: to the trigger, or to the node reading it'],
            $node instanceof Filter => ['Condition: ' . $this->name($node->function)],
            $node instanceof Until => ['Until: ' . $this->name($node->function)],
            $node instanceof WithColumn => [sprintf('Column: %s = %s', $node->name(), $this->name($node->function))],
            $node instanceof Write => ['Loader: ' . $this->name($node->loader)],
            $node instanceof Limit => ['Limit: ' . $node->limit],
            $node instanceof Offset => ['Skip: ' . $node->offset],
            $node instanceof TopN => ['Top: ' . $node->limit],
            $node instanceof Rename => [sprintf('Rename: %s → %s', $node->from, $node->to)],
            $node instanceof Select => ['Columns: ' . $this->columns($node->entries)],
            $node instanceof Drop => ['Drops: ' . $this->columns($node->entries)],
            $node instanceof Distinct => [
                'Distinct on: ' . ($node->entries === [] ? 'every column' : $this->columns($node->entries)),
            ],
            $node instanceof Batch => ['Batch size: ' . $node->size],
            $node instanceof Repartition => ['Partition by: ' . implode(', ', $node->by->names())],
            $node instanceof Sort => $node->algorithm === null
                ? ['Sort by: ' . implode(', ', $node->refs->names())]
                : [
                    'Sort by: ' . implode(', ', $node->refs->names()),
                    'Algorithm: ' . $this->algorithm($node->algorithm),
                ],
            $node instanceof Aggregate => $this->aggregate($node),
            $node instanceof Cache => $node->id === null ? [] : ['Cache: ' . $node->id],
            $node instanceof Validate => ['Against: ' . $this->name($node->validator)],
            default => [],
        };
    }

    /**
     * @return list<string>
     */
    public function join(Join $join): array
    {
        $comparison = $join->on->comparison();
        $lines = ['Type: ' . $join->type->value];

        if ($comparison instanceof Any) {
            foreach ($this->condition->lines($comparison) as $condition) {
                $lines[] = 'On: ' . $condition;
            }
        } else {
            $lines[] = 'Left on: ' . $this->references($comparison->left());
            $lines[] = 'Right on: ' . $this->references($comparison->right());
        }

        if ($join->on->prefix() !== '') {
            $lines[] = 'Prefix: ' . $join->on->prefix();
        }

        if ($join->algorithm !== null) {
            $lines[] = 'Algorithm: ' . $this->algorithm($join->algorithm);
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function crossJoin(CrossJoin $join): array
    {
        return $join->prefix === '' ? ['Type: cross'] : ['Type: cross', 'Prefix: ' . $join->prefix];
    }

    /**
     * @param array<Reference> $references
     */
    public function references(array $references): string
    {
        return implode(', ', array_map(static fn(Reference $ref): string => $ref->name(), $references));
    }

    // every algorithm is configured through a *Builder, the algorithm is what the reader is after
    public function algorithm(object $builder): string
    {
        $algorithm = $this->name($builder);

        return str_ends_with($algorithm, 'Builder') ? substr($algorithm, 0, -7) : $algorithm;
    }

    /**
     * @param list<Reference|string> $entries
     */
    public function columns(array $entries): string
    {
        return implode(', ', array_map(static fn(Reference|string $entry): string => $entry instanceof Reference
            ? $entry->name()
            : $entry, $entries));
    }

    /**
     * @return list<string>
     */
    public function aggregate(Aggregate $node): array
    {
        $lines = $node->groupBy->isGlobal()
            ? ['Group by: every row']
            : ['Group by: ' . implode(', ', $node->groupBy->refs()->names())];

        if ($node->algorithm !== null) {
            $lines[] = 'Algorithm: ' . $this->algorithm($node->algorithm);
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function read(Read $read): array
    {
        $extractor = $read->extractor();
        $lines = ['Extractor: ' . $this->name($extractor)];

        // the path is metadata the source already holds; asking for its schema would read it, which a logical stage never does
        if ($extractor instanceof FileExtractor) {
            $lines[] = 'Source: ' . $extractor->source()->uri();
        }

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
