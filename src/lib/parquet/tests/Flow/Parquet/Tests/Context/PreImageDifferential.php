<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function bin2hex;
use function Flow\Parquet\DSL\parquet_column;
use function is_string;
use function sprintf;
use function var_export;

/**
 * Each physical value against each comparison: inside the pre-image exactly when its decoded value matches.
 */
final class PreImageDifferential
{
    /**
     * Every physical value whose membership disagrees with the reference semantics, as "operator literal: value".
     *
     * @param list<bool|float|int|string> $physicals
     * @param list<bool|DateTimeImmutable|float|int|string> $literals
     *
     * @return list<string>
     */
    public static function mismatches(FlatColumn $column, array $physicals, array $literals): array
    {
        $converter = DataConverter::initialize(new Options())->resolveConverter($column);
        $mismatches = [];

        foreach ($literals as $literal) {
            foreach (Operator::cases() as $operator) {
                $intervals = PhysicalIntervalEdges::intervals($column, $operator, $literal);

                if ($intervals === null) {
                    continue;
                }

                $comparison = new Comparison(parquet_column($column->name()), $operator, $literal);

                foreach ($physicals as $physical) {
                    $matches = (new FilterEvaluator(BoundFilter::bind(
                        $comparison,
                        Schema::with($column),
                        new Options(),
                    )))->mask([
                        $column->name() => [$converter === null ? $physical : $converter->fromParquetType($physical)],
                    ]) === [0];

                    if ($matches !== $intervals->covers($physical, $physical)) {
                        $mismatches[] = sprintf(
                            '%s %s: %s',
                            $operator->name,
                            $literal instanceof DateTimeImmutable
                                ? $literal->format('Y-m-d H:i:s.uP')
                                : var_export($literal, true),
                            is_string($physical) ? bin2hex($physical) : var_export($physical, true),
                        );
                    }
                }
            }
        }

        return $mismatches;
    }
}
