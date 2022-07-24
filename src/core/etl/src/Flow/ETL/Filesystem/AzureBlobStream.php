<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Exception\RuntimeException;
use League\Flysystem\AzureBlobStorage\AzureBlobStorageAdapter;
use League\Flysystem\Filesystem;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

final class AzureBlobStream extends FlysystemWrapper
{
    public const PROTOCOL = 'flow-azure-blob';

    public static function register() : void
    {
        if (!\class_exists('League\Flysystem\AzureBlobStorage\AzureBlobStorageAdapter')) {
            throw new MissingDependencyException('Flysystem Azure Blob Adapter', 'league/flysystem-azure-blob-storage');
        }

        if (!\in_array(self::PROTOCOL, \stream_get_wrappers(), true)) {
            \stream_wrapper_register(self::PROTOCOL, self::class);
        }
    }

    /**
     * @psalm-suppress MixedArrayAccess
     */
    protected function filesystem() : Filesystem
    {
        if (!\is_resource($this->context)) {
            throw new RuntimeException(__CLASS__ . ' requires context in order to initialze filesystem');
        }

        if ($this->filesystem === null) {
            /**
             * @psalm-suppress PossiblyNullArgument
             * @psalm-suppress UndefinedThisPropertyFetch
             * @psalm-suppress MixedArgument
             */
            $contextOptions = \stream_context_get_options($this->context);

            /**
             * @psalm-suppress MixedArgument
             *
             * @var array{connection-string: string} $clientOptions
             */
            $clientOptions = \array_merge(
                ['connection-string' => ''],
                /** @phpstan-ignore-next-line */
                ['connection-string' => $contextOptions[self::PROTOCOL]['connection-string']] ?? []
            );

            /** @var string $container */
            $container = $contextOptions[self::PROTOCOL]['container'] ?? '';

            /**
             * @psalm-suppress PossiblyNullArrayAccess
             * @psalm-suppress PossiblyNullArgument
             */
            $this->filesystem = (new Filesystem(
                new AzureBlobStorageAdapter(
                    BlobRestProxy::createBlobService($clientOptions['connection-string']),
                    $container
                )
            ));
        }

        return $this->filesystem;
    }
}
