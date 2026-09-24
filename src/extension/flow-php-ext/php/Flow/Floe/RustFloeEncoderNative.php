<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Exception\ExtensionException;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustFloeEncoderNative
{
    public function __construct()
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<TypedRowValues> $batch
     * @param string $schemaBody SCHEMA frame body (JSON list of normalized definitions)
     * @param Schema $schema the same definitions as objects, so a refusal is raised through the PHP factory
     *
     * @throws ExtensionException
     *
     * @return list<string> bare ROW frame bodies
     */
    public function encode(array $batch, string $schemaBody, Schema $schema): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<string> $frameBodies
     * @param string $schemaBody SCHEMA frame body (JSON list of normalized definitions)
     *
     * @throws ExtensionException
     *
     * @return list<RawRowValues>
     */
    public function decode(array $frameBodies, string $schemaBody): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * `RustRowHydratorNative::hydrate($this->decode($frameBodies, $schemaBody), $schema)` in one pass.
     *
     * @param list<string> $frameBodies
     * @param string $schemaBody SCHEMA frame body (JSON list of normalized definitions)
     *
     * @throws ExtensionException
     */
    public function decodeRows(array $frameBodies, string $schemaBody, Schema $schema): Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * `Format::frame(Format::FRAME_ROW, ...)` over `$this->encode(RustRowHydratorNative::dehydrate($rows), ...)`.
     *
     * @param string $schemaBody SCHEMA frame body (JSON list of normalized definitions)
     *
     * @throws ExtensionException
     */
    public function encodeFrames(Rows $rows, string $schemaBody, Schema $schema): string
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
