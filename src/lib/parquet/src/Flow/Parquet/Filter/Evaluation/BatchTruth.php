<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\ExactLeaf;
use Flow\Parquet\Filter\Binding\InexactLeaf;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Filter\StartsWith;
use WeakMap;

use function array_fill;
use function array_key_exists;
use function bccomp;
use function get_debug_type;
use function is_array;
use function is_bool;
use function is_finite;
use function is_float;
use function is_int;
use function is_nan;
use function is_numeric;
use function is_string;
use function number_format;
use function sprintf;
use function str_starts_with;
use function strcmp;

/**
 * One batch's truth per row for each node of a filter; a leaf without a resolution is an absent column, null.
 *
 * @implements FilterVisitor<list<null|bool>>
 */
final readonly class BatchTruth implements FilterVisitor
{
    /**
     * @param WeakMap<Comparison|In|IsNull|StartsWith, ExactLeaf|InexactLeaf> $leaves
     * @param WeakMap<In, array<int|string, true>> $keys
     * @param array<string, list<mixed>> $batch
     * @param non-negative-int $rows
     */
    public function __construct(
        private WeakMap $leaves,
        private WeakMap $keys,
        private array $batch,
        private int $rows,
    ) {}

    public function all(All $all): array
    {
        $result = array_fill(0, $this->rows, true);

        foreach ($all->filters as $filter) {
            foreach ($filter->accept($this) as $row => $truth) {
                if ($truth === false) {
                    $result[$row] = false;
                } elseif ($truth === null && $result[$row] === true) {
                    $result[$row] = null;
                }
            }
        }

        return $result;
    }

    public function any(Any $any): array
    {
        $result = array_fill(0, $this->rows, false);

        foreach ($any->filters as $filter) {
            foreach ($filter->accept($this) as $row => $truth) {
                if ($truth === true) {
                    $result[$row] = true;
                } elseif ($truth === null && $result[$row] === false) {
                    $result[$row] = null;
                }
            }
        }

        return $result;
    }

    public function comparison(Comparison $comparison): array
    {
        $leaf = $this->leaves[$comparison] ?? null;

        if ($leaf === null) {
            return array_fill(0, $this->rows, null);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            if ($value === null) {
                $result[] = null;

                continue;
            }

            if (is_float($value) && is_nan($value)) {
                $result[] = false;

                continue;
            }

            $order = $this->order($leaf->domain, $value, $leaf->literals[0]);
            $result[] = match ($comparison->operator) {
                Operator::Equal => $order === 0,
                Operator::LessThan => $order < 0,
                Operator::LessThanOrEqual => $order <= 0,
                Operator::GreaterThan => $order > 0,
                Operator::GreaterThanOrEqual => $order >= 0,
            };
        }

        return $result;
    }

    public function in(In $in): array
    {
        $leaf = $this->leaves[$in] ?? null;

        if ($leaf === null) {
            return array_fill(0, $this->rows, null);
        }

        $keys = $this->keys[$in] ?? null;
        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            if ($value === null) {
                $result[] = null;

                continue;
            }

            $key = $keys === null ? null : $leaf->domain->key($value);

            if ($keys !== null && $key !== null) {
                $result[] = array_key_exists($key, $keys);

                continue;
            }

            $match = false;

            if (!is_float($value) || !is_nan($value)) {
                foreach ($leaf->literals as $literal) {
                    if ($this->order($leaf->domain, $value, $literal) === 0) {
                        $match = true;

                        break;
                    }
                }
            }

            $result[] = $match;
        }

        return $result;
    }

    public function isNull(IsNull $isNull): array
    {
        $leaf = $this->leaves[$isNull] ?? null;

        if ($leaf === null) {
            return array_fill(0, $this->rows, true);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            $result[] = $value === null;
        }

        return $result;
    }

    public function not(Not $not): array
    {
        $result = [];

        foreach ($not->filter->accept($this) as $truth) {
            $result[] = $truth === null ? null : !$truth;
        }

        return $result;
    }

    public function startsWith(StartsWith $startsWith): array
    {
        $leaf = $this->leaves[$startsWith] ?? null;

        if ($leaf === null) {
            return array_fill(0, $this->rows, null);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            $result[] = is_string($value) ? str_starts_with($value, $startsWith->prefix) : null;
        }

        return $result;
    }

    /**
     * The literal passed the domain check, so a decoded value orders with it.
     */
    private function order(LiteralDomain $domain, mixed $value, bool|DateTimeImmutable|float|int|string $literal): int
    {
        if ($domain === LiteralDomain::UnsignedInteger64 && is_string($value) && is_numeric($value)) {
            if (is_float($literal)) {
                if (!is_finite($literal)) {
                    return $literal > 0 ? -1 : 1;
                }

                /** @var numeric-string $decimal */
                $decimal = number_format($literal, 20, '.', '');

                return bccomp($value, $decimal, 20);
            }

            return match (true) {
                is_int($literal) => bccomp($value, (string) $literal),
                is_string($literal) && is_numeric($literal) => bccomp($value, $literal),
                default => throw self::unordered($value, $literal),
            };
        }

        return match (true) {
            is_string($value) && is_string($literal) => strcmp($value, $literal) <=> 0,
            (is_int($value) || is_float($value)) && (is_int($literal) || is_float($literal)) => $value <=> $literal,
            is_bool($value) && is_bool($literal) => $value <=> $literal,
            $value instanceof DateTimeInterface && $literal instanceof DateTimeInterface => $value <=> $literal,
            default => throw self::unordered($value, $literal),
        };
    }

    private static function unordered(mixed $value, bool|DateTimeImmutable|float|int|string $literal): RuntimeException
    {
        return new RuntimeException(sprintf(
            'A decoded %s does not order with the %s literal',
            get_debug_type($value),
            get_debug_type($literal),
        ));
    }

    /**
     * @return list<mixed>
     */
    private function values(ExactLeaf|InexactLeaf $leaf): array
    {
        $column = $this->batch[$leaf->root] ?? throw new InvalidArgumentException(
            'The batch holds no column "' . $leaf->root . '"',
        );

        if ($leaf->fields === []) {
            return $column;
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column as $value) {
            foreach ($leaf->fields as $field) {
                // @mago-ignore analysis:mixed-assignment
                $value = is_array($value) && array_key_exists($field, $value) ? $value[$field] : null;
            }

            $values[] = $value;
        }

        return $values;
    }
}
