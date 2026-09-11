<?php

declare(strict_types=1);

namespace Flow\CLI\Command\Traits;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\InfersSchema;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\CLI\option_bool;
use function Flow\CLI\option_int_nullable;
use function Flow\ETL\DSL\infer_schema;

trait SchemaInferenceOptions
{
    private function addSchemaInferenceOptions(Command $command): void
    {
        $command
            ->addOption(
                'schema-sample-size',
                null,
                InputOption::VALUE_REQUIRED,
                'Rows read to infer the schema before it is frozen; -1 reads every row. Ignored when the source declares its own schema.',
                null,
            )
            ->addOption(
                'schema-files-to-sniff',
                null,
                InputOption::VALUE_REQUIRED,
                'Sources that yielded a row count against this bound; -1 sniffs every source (leading empty files are all opened). Ignored when the source declares its own schema.',
                null,
            )
            ->addOption(
                'schema-all-strings',
                null,
                InputOption::VALUE_NONE,
                'Infer every column as string instead of narrowing it.',
            )
            ->addOption(
                'schema-union-by-name',
                null,
                InputOption::VALUE_NONE,
                'Union the column sets of every sniffed source instead of taking the first one.',
            );
    }

    private function applySchemaInference(Extractor $extractor, InputInterface $input, SymfonyStyle $style): bool
    {
        if (!$extractor instanceof InfersSchema) {
            return true;
        }

        $sampleSize = option_int_nullable('schema-sample-size', $input);
        $filesToSniff = option_int_nullable('schema-files-to-sniff', $input);

        if ($sampleSize !== null && $sampleSize < 1 && $sampleSize !== -1) {
            $style->error('Schema sample size must be greater than 0, or -1 for all rows.');

            return false;
        }

        if ($filesToSniff !== null && $filesToSniff < 1 && $filesToSniff !== -1) {
            $style->error('Schema files to sniff must be greater than 0, or -1 for all sources.');

            return false;
        }

        $inference = infer_schema();

        if ($sampleSize !== null) {
            $inference = $inference->sampleSize($sampleSize);
        }

        if ($filesToSniff !== null) {
            $inference = $inference->filesToSniff($filesToSniff);
        }

        if (option_bool('schema-all-strings', $input)) {
            $inference = $inference->allStrings();
        }

        if (option_bool('schema-union-by-name', $input)) {
            $inference = $inference->unionByName();
        }

        $extractor->inferSchema($inference);

        return true;
    }
}
