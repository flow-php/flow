<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\SchemaValidator;
use NoDiscard;

final readonly class MatchSchema implements Transformation
{
    /**
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     */
    public function __construct(
        private Schema $schema,
        private ?SchemaValidator $validator = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->match($this->schema, $this->validator);
    }
}
