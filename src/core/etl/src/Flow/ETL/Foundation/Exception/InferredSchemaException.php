<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use function array_diff;
use function implode;
use function sprintf;

final class InferredSchemaException extends InvalidArgumentException
{
    /**
     * @param list<string> $expected - the column names of the inferred schema
     * @param list<string> $columns - the columns the source actually carries - its header, duplicates already collapsed
     */
    public static function columnsDiverge(
        string $source,
        string $inferredFrom,
        array $expected,
        array $columns,
        ?int $sampleSize,
        ?int $filesToSniff,
    ): self {
        return new self(sprintf(
            'Columns of %s do not match the schema inferred from %s: unexpected [%s], missing [%s]. The sample was '
            . 'at most %s rows over at most %s sources. Declare the schema with ->withSchema(...), or read the '
            . 'sources as one wider schema with ->inferSchema(infer_schema()->unionByName()).',
            $source,
            $inferredFrom,
            implode(', ', array_diff($columns, $expected)),
            implode(', ', array_diff($expected, $columns)),
            $sampleSize ?? 'all',
            $filesToSniff ?? 'all',
        ));
    }

    /**
     * @param string $readFrom - the file the extractor's schema was read from, the first one it listed
     */
    public static function filesDiverge(string $source, string $readFrom, string $mismatches): self
    {
        return new self(sprintf(
            "Columns of %s do not match the schema read from %s:\n%sRead the files as one wider schema with "
            . '->unionByName().',
            $source,
            $readFrom,
            $mismatches,
        ));
    }

    /**
     * @param string $source the DSL call that built the source, e.g. 'from_parquet()'
     * @param string $reason why nothing was there, e.g. 'no row carries a column'
     */
    public static function nothingToInferFrom(string $source, string $reason): self
    {
        return new self(sprintf(
            '%s has nothing to infer a schema from: %s. Declare the schema with ->withSchema(...).',
            $source,
            $reason,
        ));
    }

    /**
     * A file's record that does not fit a schema inferred from a sample - the sample can span files, so no position
     * in this file places the record inside it.
     */
    public static function outsideTheSample(
        string $uri,
        int $offset,
        ?int $line,
        ?int $sampleSize,
        SchemaMismatchException $mismatch,
    ): self {
        return new self(
            sprintf(
                'Record at offset %d%s of %s does not fit the schema inferred from a sample of %s rows: %s. '
                . 'Infer from more rows and files with ->inferSchema(infer_schema()->sampleSize(...)->filesToSniff(...)), '
                . 'or declare the schema with ->withSchema(...).',
                $offset,
                $line === null ? '' : ", line {$line},",
                $uri,
                $sampleSize ?? 'all',
                $mismatch->cause->reason(),
            ),
            0,
            $mismatch,
        );
    }

    /**
     * The refusal to raise for $mismatch, a row at its 0-based position in the whole source: past a bounded sample it
     * becomes the way out, within the sample (or with no inference behind the schema) it stays the gate's own.
     *
     * @param null|int $sampleSize the rows the schema was inferred from; null when there is no bound to outrun (a
     *                             declared schema, or one inferred from every row)
     */
    public static function whenPastTheSample(
        string $source,
        ?int $sampleSize,
        SchemaMismatchException $mismatch,
    ): self|SchemaMismatchException {
        if ($sampleSize === null || $mismatch->rowIndex < $sampleSize) {
            return $mismatch;
        }

        return new self(
            sprintf(
                'Row %d of %s does not fit the schema inferred from its first %d rows: %s. Infer from '
                . 'every row with ->inferSchema(infer_schema()->everyRow()), or declare the schema with '
                . '->withSchema(...).',
                $mismatch->rowIndex,
                $source,
                $sampleSize,
                $mismatch->cause->reason(),
            ),
            0,
            $mismatch,
        );
    }
}
