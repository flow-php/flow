<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\Functions\Join\Expression as JoinExpression;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Filesystem\ScalarFunctionFilter;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Planning\Logical\Node\Batch;
use Flow\ETL\Planning\Logical\Node\Cache;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Distinct;
use Flow\ETL\Planning\Logical\Node\Drop;
use Flow\ETL\Planning\Logical\Node\ExpandColumn;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Join;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Rename;
use Flow\ETL\Planning\Logical\Node\Repartition;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Node\Until;
use Flow\ETL\Planning\Logical\Node\Validate;
use Flow\ETL\Planning\Logical\Node\WithColumn;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\SortOrder;
use Flow\Filesystem\Path\Filter as PathFilter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Telemetry\ObjectExtractor;

use function array_map;
use function array_merge;
use function implode;
use function sprintf;
use function str_ends_with;
use function strtolower;
use function substr;

final readonly class NodeDetails
{
    public function __construct(
        private Condition $condition = new Condition(),
        private Expression $expression = new Expression(),
    ) {}

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
            $node instanceof Join => $node->algorithm === null
                ? $this->join($node->type, $node->on)
                : [...$this->join($node->type, $node->on), 'Algorithm: ' . $this->algorithm($node->algorithm)],
            $node instanceof CrossJoin => $this->crossJoin($node->prefix),
            $node instanceof Result => ['Rows this plan hands out: to the trigger, or to the node reading it'],
            $node instanceof Filter => ['Condition: ' . $this->expression->of($node->function)],
            $node instanceof Until => ['Until: ' . $this->expression->of($node->function)],
            $node instanceof WithColumn => [
                sprintf('Column: %s = %s', $node->name(), $this->expression->of($node->function)),
            ],
            $node instanceof ExpandColumn => $this->expandColumn($node),
            $node instanceof Write => ['Loader: ' . ObjectExtractor::shortName($node->loader)],
            $node instanceof Limit => ['Limit: ' . $node->limit],
            $node instanceof Offset => ['Skip: ' . $node->offset],
            $node instanceof TopN => ['Top: ' . $node->limit],
            $node instanceof Rename => [sprintf('Rename: %s → %s', $node->from, $node->to)],
            $node instanceof Select => ['Keeps only: ' . $this->columns($node->columns)],
            $node instanceof Drop => ['Drops: ' . $this->columns($node->columns)],
            $node instanceof Distinct => [
                'Distinct on: ' . ($node->columns === [] ? 'every column' : $this->columns($node->columns)),
            ],
            $node instanceof Batch => ['Batch size: ' . $node->size->size],
            $node instanceof Repartition => ['Partition by: ' . $this->columns($node->by->all())],
            $node instanceof Sort => $node->algorithm === null
                ? ['Sort by: ' . $this->sort($node->columns)]
                : ['Sort by: ' . $this->sort($node->columns), 'Algorithm: ' . $this->algorithm($node->algorithm)],
            $node instanceof Aggregate => $this->aggregate($node),
            $node instanceof Cache => $node->id === null ? [] : ['Cache: ' . $node->id],
            $node instanceof Validate => ['Against: ' . ObjectExtractor::shortName($node->validator)],
            default => [],
        };
    }

    /**
     * @return list<string> the type, one line per condition the rows must meet, the prefix when set
     */
    public function join(JoinType $type, JoinExpression $on): array
    {
        $lines = ['Type: ' . $type->value];

        foreach ($this->condition->lines($on->comparison()) as $condition) {
            $lines[] = 'On: ' . $condition;
        }

        return $on->prefix() === '' ? $lines : [...$lines, 'Prefix: ' . $on->prefix()];
    }

    /**
     * @return list<string>
     */
    public function crossJoin(string $prefix): array
    {
        return $prefix === '' ? ['Type: cross'] : ['Type: cross', 'Prefix: ' . $prefix];
    }

    /**
     * @param list<SortColumn> $columns
     */
    public function sort(array $columns): string
    {
        return implode(', ', array_map(
            static fn(SortColumn $column): string => (
                $column->name
                . ' '
                . ($column->order === SortOrder::ASC ? 'asc' : 'desc')
                . (
                    $column->nulls === NullsOrder::defaultFor($column->order)
                        ? ''
                        : ' nulls ' . strtolower($column->nulls->name)
                )
            ),
            $columns,
        ));
    }

    // every algorithm is configured through a *Builder, the algorithm is what the reader is after
    public function algorithm(object $builder): string
    {
        $algorithm = ObjectExtractor::shortName($builder);

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
    public function expandColumn(ExpandColumn $node): array
    {
        $lines = [sprintf('Column: %s = %s', $node->name(), $this->expression->of($node->function))];

        return match (true) {
            $node->carries->isAll() => $lines,
            $node->carries->allBut => [...$lines, 'Keeps: every column but ' . implode(', ', $node->carries->names)],
            $node->carries->names === [] => [...$lines, 'Keeps: no input column'],
            default => [...$lines, 'Keeps: ' . implode(', ', $node->carries->names)],
        };
    }

    /**
     * @return list<string>
     */
    public function aggregate(Aggregate $node): array
    {
        $lines = $node->groupBy->isGlobal()
            ? ['Group by: every row']
            : ['Group by: ' . $this->columns($node->groupBy->refs()->all())];

        if ($node->algorithm !== null) {
            $lines[] = 'Algorithm: HashGroupBy';
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function read(Read $read): array
    {
        $extractor = $read->extractor();
        $lines = ['Extractor: ' . ObjectExtractor::shortName($extractor)];

        // the path is metadata the source already holds; asking for its schema would read it, which a logical stage never does
        if ($extractor instanceof FileExtractor) {
            $lines[] = 'Source: ' . $extractor->source()->uri();
        }

        return [...$lines, ...$this->criteria($read->criteria())];
    }

    /**
     * What the plan pushed into a read, each conjunct the source applies on its own line.
     *
     * @return list<string>
     */
    public function criteria(Criteria $criteria): array
    {
        $lines = [];

        if ($criteria->limit !== null) {
            $lines[] = 'Limit: ' . $criteria->limit;
        }

        foreach ($this->files($criteria->pathFilter) as $filter) {
            $lines[] = 'Files: ' . $filter;
        }

        foreach ($criteria->conjuncts as $conjunct) {
            $lines[] = 'Filter: ' . $this->expression->of($conjunct);
        }

        return $lines;
    }

    /**
     * Each filter a file must pass, one per line.
     *
     * @return list<string>
     */
    public function files(PathFilter $filter): array
    {
        return match (true) {
            // every file source reads only files, saying so on every read would only add noise
            $filter instanceof OnlyFiles => [],
            $filter instanceof Filters => array_merge(...array_map($this->files(...), $filter->filters())),
            $filter instanceof ScalarFunctionFilter => [$this->expression->of($filter->function())],
            default => [ObjectExtractor::shortName($filter)],
        };
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

        if ($redefines->isUnknown()) {
            $notes[] = 'Changes columns known only at run time';
        } elseif (!$redefines->isAllBut() && $redefines->names !== []) {
            $notes[] = 'Changes columns: ' . implode(', ', $redefines->names);
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
        ];

        if ($redefines->isUnknown()) {
            $declarations[] = 'redefines unknown';
        } elseif ($redefines->isAllBut()) {
            $declarations[] = 'redefines all but ' . implode(', ', $redefines->names);
        } elseif ($redefines->names !== []) {
            $declarations[] = 'redefines ' . implode(', ', $redefines->names);
        }

        return implode(' · ', $declarations);
    }
}
