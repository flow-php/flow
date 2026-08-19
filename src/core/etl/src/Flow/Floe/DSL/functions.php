<?php

declare(strict_types=1);

namespace Flow\Floe\DSL;

use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Path;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeEngine;
use Flow\Floe\FloeExtractor;
use Flow\Floe\FloeLoader;
use Flow\Floe\FloeMerger;
use Flow\Floe\Options;

use function array_map;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function is_string;

/**
 * @param Path|string $path
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::EXTRACTOR)]
function from_floe(
    string|Path $path,
    Codec $codec = new NoopCodec(),
    int $chunk_size = 65536,
    FloeEngine $engine = FloeEngine::adaptive,
): FloeExtractor {
    return new FloeExtractor(is_string($path) ? path_real($path) : $path, $codec, $chunk_size, $engine);
}

/**
 * @param Path|string $path
 * @param Options $options see floe_options() - validateData gates the per-value type check
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::LOADER)]
function to_floe(
    string|Path $path,
    ?Metadata $metadata = null,
    Options $options = new Options(),
    FloeEngine $engine = FloeEngine::adaptive,
): FloeLoader {
    return new FloeLoader(is_string($path) ? path_real($path) : $path, $metadata, $options, $engine);
}

/**
 * @param bool $validate_data gates the per-value type check on write. The column-set check -
 *                            a row carrying a column the session schema does not declare -
 *                            always runs. Mirrors parquet-java's ParquetWriter::withValidation().
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::HELPER)]
function floe_options(bool $validate_data = true, int $buffer_size = 65536, Codec $codec = new NoopCodec()): Options
{
    return new Options($validate_data, $buffer_size, $codec);
}

/**
 * Merges several Floe files (same or append-compatible evolving schema) into one, on the local
 * filesystem. Byte-splices frame regions by default (O(bytes), no re-encode); compact re-encodes
 * all rows into fewer sections. For non-local filesystems use FloeMerger directly.
 *
 * @param array<int, Path|string> $sources
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::HELPER)]
function merge_floe(array $sources, string|Path $dest, bool $compact = false, ?Metadata $metadata = null): void
{
    (new FloeMerger(native_local_filesystem()))->merge(
        array_map(static fn(string|Path $source): Path => is_string($source) ? path_real($source) : $source, $sources),
        is_string($dest) ? path($dest) : $dest,
        $compact,
        $metadata,
    );
}
