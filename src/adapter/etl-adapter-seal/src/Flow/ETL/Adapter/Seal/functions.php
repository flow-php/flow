<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;

/**
 * Loads Flow Rows into a SEAL index. Each Row is turned into a document via Row::toArray(); the identifier
 * value must be present as one of the Row entries (declared as IdentifierField in the SEAL schema).
 *
 * Writes are fire-and-forget by default for throughput. Use DataFrame::chunkSize() to control how many rows
 * are passed to a single bulk() call, and SealLoader::withBulkSize() to tune SEAL's internal sub-batching.
 *
 * @param EngineInterface $engine pre-built SEAL engine (new Engine($adapter, $schema))
 * @param string $index target index name as declared in the SEAL schema
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::LOADER)]
function to_seal(EngineInterface $engine, string $index): SealLoader
{
    return new SealLoader($engine, $index);
}

/**
 * Extracts documents from a SEAL index as Flow Rows using limit/offset pagination. Use
 * SealExtractor::withSearchBuilder() to add filters/sorting through the native SEAL SearchBuilder.
 *
 * @param EngineInterface $engine pre-built SEAL engine (new Engine($adapter, $schema))
 * @param string $index source index name as declared in the SEAL schema
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::EXTRACTOR)]
function from_seal(EngineInterface $engine, string $index): SealExtractor
{
    return new SealExtractor($engine, $index);
}

/**
 * Creates a single index on the configured SEAL engine and blocks until the task is completed.
 *
 * @param EngineInterface $engine pre-built SEAL engine
 * @param string $index index name to create
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_create_index(EngineInterface $engine, string $index): void
{
    $engine->createIndex($index, ['return_slow_promise_result' => true])?->wait();
}

/**
 * Drops a single index on the configured SEAL engine and blocks until the task is completed.
 *
 * @param EngineInterface $engine pre-built SEAL engine
 * @param string $index index name to drop
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_drop_index(EngineInterface $engine, string $index): void
{
    $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
}

/**
 * Creates all indexes defined in the SEAL engine's schema and blocks until the tasks are completed.
 *
 * @param EngineInterface $engine pre-built SEAL engine
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_create_schema(EngineInterface $engine): void
{
    $engine->createSchema(['return_slow_promise_result' => true])?->wait();
}

/**
 * Drops all indexes defined in the SEAL engine's schema and blocks until the tasks are completed.
 *
 * @param EngineInterface $engine pre-built SEAL engine
 */
#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_drop_schema(EngineInterface $engine): void
{
    $engine->dropSchema(['return_slow_promise_result' => true])?->wait();
}
