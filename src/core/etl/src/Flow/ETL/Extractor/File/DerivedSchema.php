<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\StrictValidator;

/**
 * The schema a self-describing format derived from its first file, which every file it reads must match.
 */
final readonly class DerivedSchema
{
    /**
     * @param string $derivedFrom the file the schema was read from
     */
    public function __construct(
        private Schema $derived,
        private string $derivedFrom,
    ) {}

    /**
     * @throws InferredSchemaException
     */
    public function refuseDivergence(string $source, Schema $schema): void
    {
        $validation = (new StrictValidator())->validate($this->derived, $schema);

        if (!$validation->isValid()) {
            throw InferredSchemaException::filesDiverge($source, $this->derivedFrom, $validation);
        }
    }
}
