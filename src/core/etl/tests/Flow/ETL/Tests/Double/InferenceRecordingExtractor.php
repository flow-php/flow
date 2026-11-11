<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\InfersSchema;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\schema;

final class InferenceRecordingExtractor implements Extractor, InfersSchema
{
    /**
     * @var list<Progress> every Progress handed to reportInferenceTo(), in call order
     */
    public array $reportedTo = [];

    private Progress $inferenceProgress;

    public function __construct()
    {
        $this->inferenceProgress = new VoidProgress();
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield Rows::empty($this->schema(), new AdaptiveBackend());
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        return $this;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    public function reportInferenceTo(Progress $progress): void
    {
        $this->reportedTo[] = $progress;
        $this->inferenceProgress = $progress;
    }

    public function schema(): Schema
    {
        $this->inferenceProgress->advanced(1);

        return schema(integer_schema('id'));
    }

    public function statistics(): Statistics
    {
        return new Statistics(rows: Cardinality::unknown(), size: Cardinality::unknown());
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
