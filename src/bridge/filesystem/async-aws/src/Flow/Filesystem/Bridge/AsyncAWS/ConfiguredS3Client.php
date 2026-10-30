<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Configuration;
use AsyncAws\S3\S3Client;
use SensitiveParameter;

final class ConfiguredS3Client implements S3ClientSource
{
    private ?S3Client $client = null;

    /**
     * @param array<Configuration::OPTION_*, null|string> $configuration kept verbatim: env, profile and IMDS re-resolve in every process
     */
    public function __construct(
        #[SensitiveParameter]
        private readonly array $configuration,
    ) {}

    public function client(): S3Client
    {
        return $this->client ??= new S3Client($this->configuration);
    }

    /**
     * @return array<Configuration::OPTION_*, null|string>
     */
    public function normalize(): array
    {
        return $this->configuration;
    }
}
