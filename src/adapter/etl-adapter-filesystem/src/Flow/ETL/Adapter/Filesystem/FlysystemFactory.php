<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem;

use Aws\S3\S3Client;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Filesystem\Path;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use League\Flysystem\AzureBlobStorage\AzureBlobStorageAdapter;
use League\Flysystem\Filesystem;
use League\Flysystem\Local\LocalFilesystemAdapter;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

final class FlysystemFactory
{
    public function create(Path $path) : Filesystem
    {
        if ($path->isLocal()) {
            return $this->local();
        }

        return match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };
    }

    /**
     * @throws MissingDependencyException
     * @throws InvalidArgumentException
     */
    private function aws(Path $path) : Filesystem
    {
        AwsS3Stream::register();

        $options = $path->options();

        $expectedOptions = '["client" => ["credentials" => ["key" => "__key__", "secret" => "__secret__"], "region" => "eu-west-2", "version" => "latest"], "bucket" => "__name__"]';

        if (!\array_key_exists('client', $options)) {
            throw new InvalidArgumentException("Missing AWS client in Path options, expected options: {$expectedOptions}");
        }

        if (!\array_key_exists('bucket', $options)) {
            throw new InvalidArgumentException("Missing AWS bucket in Path options, expected options: {$expectedOptions}");
        }

        return new Filesystem(new AwsS3V3Adapter(new S3Client($options['client']), $options['bucket']));
    }

    /**
     * @throws MissingDependencyException
     * @throws InvalidArgumentException
     */
    private function azure(Path $path) : Filesystem
    {
        AzureBlobStream::register();

        $options = $path->options();

        $expectedOptions = '["connection-string" => "__connection_string___", "container" => "__container__"]';

        if (!\array_key_exists('connection-string', $options)) {
            throw new InvalidArgumentException("Missing Azure Blob connection-string in Path options, expected options: {$expectedOptions}");
        }

        if (!\array_key_exists('container', $options)) {
            throw new InvalidArgumentException("Missing Azure Blob container in Path options, expected options: {$expectedOptions}");
        }

        return new Filesystem(new AzureBlobStorageAdapter(BlobRestProxy::createBlobService($options['connection-string']), $options['container']));
    }

    private function local() : Filesystem
    {
        return new Filesystem(new LocalFilesystemAdapter(DIRECTORY_SEPARATOR, linkHandling: LocalFilesystemAdapter::SKIP_LINKS));
    }
}
