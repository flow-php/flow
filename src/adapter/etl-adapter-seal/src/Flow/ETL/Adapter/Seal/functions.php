<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema as SealSchema;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\ETL\Schema;

#[DocumentationDSL(module: Module::SEAL, type: Type::LOADER)]
function to_seal_upsert(EngineInterface $engine, string $index): SealLoader
{
    return new SealLoader($engine, $index, Operation::UPSERT);
}

#[DocumentationDSL(module: Module::SEAL, type: Type::LOADER)]
function to_seal_delete(EngineInterface $engine, string $index): SealLoader
{
    return new SealLoader($engine, $index, Operation::DELETE);
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
