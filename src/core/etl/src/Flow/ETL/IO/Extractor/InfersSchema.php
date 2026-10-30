<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\IO\Inference\SchemaInferenceBuilder;

interface InfersSchema
{
    public function inferSchema(SchemaInferenceBuilder $builder): static;
}
