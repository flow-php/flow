<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Aws\S3\S3Client;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Exception\RuntimeException;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use League\Flysystem\Filesystem;

final class AwsS3Stream extends FlysystemWrapper
{
    public const PROTOCOL = 'flow-aws-s3';

    public static function register() : void
    {
        if (!\class_exists('League\Flysystem\AwsS3V3\AwsS3V3Adapter')) {
            throw new MissingDependencyException('Flysystem AWS S3 Adapter', 'league/flysystem-aws-s3-v3');
        }

        if (!\in_array(self::PROTOCOL, \stream_get_wrappers(), true)) {
            \stream_wrapper_register(self::PROTOCOL, self::class, STREAM_IS_URL);
        }
    }

    protected function filesystem() : Filesystem
    {
        if (!\is_resource($this->context)) {
            throw new RuntimeException(__CLASS__ . ' requires context in order to initialize filesystem');
        }

        if ($this->filesystem === null) {
            /**
             * @psalm-suppress PossiblyNullArgument
             * @psalm-suppress UndefinedThisPropertyFetch
             */
            $contextOptions = \stream_context_get_options($this->context);

            /**
             * @var array{credentials: array{key: string, secret: string}, region: string, version: string} $clientOptions
             */
            $clientOptions = \array_merge(
                [
                    'credentials' => [
                        'key'    => '',
                        'secret' => '',
                    ],
                    'region' => '',
                    'version' => 'latest',
                ],
                $contextOptions[self::PROTOCOL]['client'] ?? []
            );

            /** @var string $bucket */
            $bucket = $contextOptions[self::PROTOCOL]['bucket'] ?? '';

            /**
             * @psalm-suppress PossiblyNullArrayAccess
             * @psalm-suppress PossiblyNullArgument
             */
            $this->filesystem = (new Filesystem(new AwsS3V3Adapter(new S3Client($clientOptions), $bucket)));
        }

        return $this->filesystem;
    }
}
