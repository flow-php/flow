<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\DSL;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Mount;

#[DocumentationDSL(module: Module::AZURE_FILESYSTEM, type: Type::HELPER)]
function azure_filesystem_options(): Options
{
    return new Options();
}

#[DocumentationDSL(module: Module::AZURE_FILESYSTEM, type: Type::HELPER)]
function azure_filesystem(
    BlobServiceInterface $blob_service,
    Options $options = new Options(),
    string $protocol = 'azure-blob',
): AzureBlobFilesystem {
    return new AzureBlobFilesystem(new Mount($protocol), $blob_service, $options);
}
