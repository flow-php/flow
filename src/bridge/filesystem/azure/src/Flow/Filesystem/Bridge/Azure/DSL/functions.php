<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\DSL;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\ETL\Attribute\{DSL, Module, Type};
use Flow\Filesystem\Bridge\Azure\{AzureBlobFilesystem, Options};

#[DSL(module: Module::AZURE_FILESYSTEM, type: Type::HELPER)]
function azure_filesystem_options() : Options
{
    return new Options();
}

#[DSL(module: Module::AZURE_FILESYSTEM, type: Type::HELPER)]
function azure_filesystem(BlobServiceInterface $blob_service, Options $options = new Options()) : AzureBlobFilesystem
{
    return new AzureBlobFilesystem($blob_service, $options);
}
