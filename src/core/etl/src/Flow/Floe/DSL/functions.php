<?php

declare(strict_types=1);

namespace Flow\Floe\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeEngine;
use Flow\Floe\FloeExtractor;
use Flow\Floe\FloeLoader;
use Flow\Floe\FloeMerger;
use Flow\Floe\Options;

use function array_map;
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
    Filesystem $filesystem = new NativeLocalFilesystem(),
): FloeExtractor {
    return new FloeExtractor(is_string($path) ? path_real($path) : $path, $codec, $chunk_size, $engine, $filesystem);
}

/**
 * @param Path|string $path
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::LOADER)]
function to_floe(
    string|Path $path,
    ?Metadata $metadata = null,
    Options $options = new Options(),
    FloeEngine $engine = FloeEngine::adaptive,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): FloeLoader {
    return new FloeLoader(is_string($path) ? path_real($path) : $path, $metadata, $options, $engine, $filesystem);
}

#[DocumentationDSL(module: Module::FLOE, type: DSLType::HELPER)]
function floe_options(int $buffer_size = 65536, Codec $codec = new NoopCodec()): Options
{
    return new Options($buffer_size, $codec);
}

/**
 * Merges several Floe files (same or append-compatible evolving schema) into one. Byte-splices frame
 * regions by default (O(bytes), no re-encode); compact re-encodes all rows into fewer sections.
 *
 * @param array<int, Path|string> $sources
 */
#[DocumentationDSL(module: Module::FLOE, type: DSLType::HELPER)]
function merge_floe(
    array $sources,
    string|Path $dest,
    bool $compact = false,
    ?Metadata $metadata = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): void {
    (new FloeMerger($filesystem))->merge(
        array_map(static fn(string|Path $source): Path => is_string($source) ? path_real($source) : $source, $sources),
        is_string($dest) ? path($dest) : $dest,
        $compact,
        $metadata,
    );
}
