<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;

use function array_diff;
use function implode;
use function sprintf;

final class InferredSchemaException extends InvalidArgumentException
{
    /**
     * @param list<string> $columns - the columns the source actually carries - its header, duplicates already collapsed
     */
    public static function columnsDiverge(
        string $source,
        string $inferredFrom,
        Schema $inferred,
        array $columns,
        SchemaInference $inference,
    ): self {
        $expected = $inferred->references()->names();

        return new self(sprintf(
            'Columns of %s do not match the schema inferred from %s: unexpected [%s], missing [%s]. The sample was '
            . 'at most %s rows over at most %s sources. Declare the schema with ->withSchema(...), or read the '
            . 'sources as one wider schema with ->inferSchema(infer_schema()->unionByName()).',
            $source,
            $inferredFrom,
            implode(', ', array_diff($columns, $expected)),
            implode(', ', array_diff($expected, $columns)),
            $inference->sampleSize === -1 ? 'all' : (string) $inference->sampleSize,
            $inference->filesToSniff === -1 ? 'all' : (string) $inference->filesToSniff,
        ));
    }

    /**
     * @param int $row - the row's 0-based position in the whole source, not in the batch the gate refused
     */
    public static function pastTheSample(
        string $source,
        int $row,
        SchemaInference $inference,
        SchemaMismatchException $mismatch,
    ): self {
        return new self(
            sprintf(
                'Row %d of %s does not fit the schema inferred from its first %d rows: column "%s"%s. Infer from '
                . 'every row with ->inferSchema(infer_schema()->sampleSize(-1)), or declare the schema with '
                . '->withSchema(...).',
                $row,
                $source,
                $inference->sampleSize,
                $mismatch->cause->column,
                $mismatch->cause->detail,
            ),
            0,
            $mismatch,
        );
    }
}
