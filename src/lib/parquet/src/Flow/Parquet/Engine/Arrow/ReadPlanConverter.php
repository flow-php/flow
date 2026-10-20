<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Reader\ReadPlan;

use function array_map;

/**
 * @implements FilterVisitor<array<string, mixed>>
 */
final readonly class ReadPlanConverter implements FilterVisitor
{
    private function __construct(
        private BoundFilter $filter,
    ) {}

    /**
     * Called by name from arrow-ext RustBatchReader::__construct() and RustColumnsReader::__construct(), pinned by
     * phpt 055.
     *
     * @return array{selectors: list<array{0: int, 1: bool}>, filter?: array<string, mixed>, limit: ?int} file-absolute
     *         selectors (row count, skip), the row filter's tree and the limit counted after it
     */
    public static function toExtension(ReadPlan $plan): array
    {
        $converted = [
            'selectors' => array_map(static fn($selector): array => [
                $selector->rowCount,
                $selector->skip,
            ], $plan->selection->selectors()),
        ];

        if ($plan->rowFilter !== null) {
            $converted['filter'] = $plan->rowFilter->filter()->accept(new self($plan->rowFilter));
        }

        $converted['limit'] = $plan->limit;

        return $converted;
    }

    public function all(All $all): array
    {
        return ['and' => $this->children($all->filters)];
    }

    public function any(Any $any): array
    {
        return ['or' => $this->children($any->filters)];
    }

    public function comparison(Comparison $comparison): array
    {
        return $this->intervals($comparison);
    }

    public function in(In $in): array
    {
        return $this->intervals($in);
    }

    public function isNull(IsNull $isNull): array
    {
        $resolution = $this->filter->rowLeaf($isNull);

        return (
            $resolution instanceof AbsentLeaf
                ? ['leaf' => 'absent', 'is_null' => true]
                : ['leaf' => 'is_null', 'path' => $resolution->column->path()]
        );
    }

    public function not(Not $not): array
    {
        return ['not' => $not->filter->accept($this)];
    }

    public function startsWith(StartsWith $startsWith): array
    {
        $resolution = $this->filter->rowLeaf($startsWith);

        return (
            $resolution instanceof AbsentLeaf
                ? ['leaf' => 'absent', 'is_null' => false]
                : ['leaf' => 'starts_with', 'path' => $resolution->column->path(), 'prefix' => $startsWith->prefix]
        );
    }

    /**
     * @param list<Filter> $filters
     *
     * @return list<array<string, mixed>>
     */
    private function children(array $filters): array
    {
        return array_map(fn(Filter $child): array => $child->accept($this), $filters);
    }

    /**
     * @return array<string, mixed>
     */
    private function intervals(Comparison|In $node): array
    {
        $resolution = $this->filter->rowLeaf($node);

        if ($resolution instanceof AbsentLeaf) {
            return ['leaf' => 'absent', 'is_null' => false];
        }

        /** @var PhysicalIntervals $intervals rowLeaf() resolves only leaves answered exactly */
        $intervals = $resolution->intervals;

        return [
            'leaf' => 'intervals',
            'path' => $resolution->column->path(),
            'intervals' => array_map(static fn($interval): array => [
                $interval->lower,
                $interval->lowerInclusive,
                $interval->upper,
                $interval->upperInclusive,
            ], $intervals->intervals),
        ];
    }
}
