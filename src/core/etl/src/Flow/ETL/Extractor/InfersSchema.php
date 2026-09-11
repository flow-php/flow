<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;

interface InfersSchema
{
    public function inferSchema(SchemaInferenceBuilder $builder): static;
}
