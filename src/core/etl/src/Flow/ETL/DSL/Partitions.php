<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Partition as FlowPartition;
use Flow\ETL\Partition\CallableFilter;
use Flow\ETL\Partition\PartitionFilter;

/**
 * @deprecated please pass ScalarFunctions directly to DataFrame::partitionFilter() method
 *
 * @infection-ignore-all
 */
class Partitions
{
    public static function chain(PartitionFilter ...$filters) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($filters) : bool {
            foreach ($filters as $filter) {
                if (!$filter->keep(...$partitions)) {
                    return false;
                }
            }

            return true;
        });
    }

    public static function date_after(string $partition, \DateTimeInterface $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && new \DateTimeImmutable($p->value) > $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function date_after_or_equal(string $partition, \DateTimeInterface $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && new \DateTimeImmutable($p->value) >= $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function date_before(string $partition, \DateTimeInterface $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && new \DateTimeImmutable($p->value) < $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function date_before_or_equal(string $partition, \DateTimeInterface $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && new \DateTimeImmutable($p->value) <= $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function date_between(string $partition, \DateTimeInterface $start, \DateTimeInterface $end) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $start, $end) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && new \DateTimeImmutable($p->value) >= $start && new \DateTimeImmutable($p->value) < $end) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function greater(string $partition, int|float $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                $castedValue = \is_int($value) ? (int) $p->value : (float) $p->value;

                if ($p->name === $partition && $castedValue > $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function greater_or_equal(string $partition, int|float $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                $castedValue = \is_int($value) ? (int) $p->value : (float) $p->value;

                if ($p->name === $partition && $castedValue >= $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function lower(string $partition, int|float $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                $castedValue = \is_int($value) ? (int) $p->value : (float) $p->value;

                if ($p->name === $partition && $castedValue < $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function lower_or_equal(string $partition, int|float $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                $castedValue = \is_int($value) ? (int) $p->value : (float) $p->value;

                if ($p->name === $partition && $castedValue <= $value) {
                    return true;
                }
            }

            return false;
        });
    }

    public static function not(PartitionFilter $filter) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static fn (FlowPartition ...$partitions) : bool => !$filter->keep(...$partitions));
    }

    /**
     * @param string $partition
     * @param array<mixed> $values
     *
     * @return PartitionFilter
     */
    public static function one_of(string $partition, array $values) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(
            static function (FlowPartition ...$partitions) use ($partition, $values) : bool {
                foreach ($partitions as $p) {
                    if ($p->name === $partition && \in_array($p->value, $values, true)) {
                        return true;
                    }
                }

                return false;
            }
        );
    }

    public static function only(string $partition, string $value) : PartitionFilter
    {
        /** @psalm-suppress DeprecatedClass */
        return new CallableFilter(static function (FlowPartition ...$partitions) use ($partition, $value) : bool {
            foreach ($partitions as $p) {
                if ($p->name === $partition && $p->value === $value) {
                    return true;
                }
            }

            return false;
        });
    }
}
