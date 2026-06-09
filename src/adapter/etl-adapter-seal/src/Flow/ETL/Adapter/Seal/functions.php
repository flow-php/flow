<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema as SealSchema;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;
use Flow\ETL\Schema;

#[DocumentationDSL(module: Module::SEAL, type: Type::LOADER)]
function to_seal(EngineInterface $engine, string $index): SealLoader
{
    return new SealLoader($engine, $index);
}

#[DocumentationDSL(module: Module::SEAL, type: Type::EXTRACTOR)]
function from_seal(EngineInterface $engine, string $index): SealExtractor
{
    return new SealExtractor($engine, $index);
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_create_index(EngineInterface $engine, string $index): void
{
    $engine->createIndex($index, ['return_slow_promise_result' => true])?->wait();
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_drop_index(EngineInterface $engine, string $index): void
{
    $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_create_schema(EngineInterface $engine): void
{
    $engine->createSchema(['return_slow_promise_result' => true])?->wait();
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_drop_schema(EngineInterface $engine): void
{
    $engine->dropSchema(['return_slow_promise_result' => true])?->wait();
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function to_seal_schema(Schema $schema, string $index_name, ?string $identifier = null): SealSchema
{
    return (new SchemaConverter())->toSealSchema($schema, $index_name, $identifier);
}

#[DocumentationDSL(module: Module::SEAL, type: Type::HELPER)]
function seal_schema_to_flow(SealSchema $schema): Schema
{
    return (new SchemaConverter())->toFlowSchema($schema);
}
