<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Configuration;
use AsyncAws\Core\HttpClient\AwsRetryStrategy;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Exception\InvalidArgumentException;
use SensitiveParameter;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\HttpClient\RetryableHttpClient;

use function array_diff;
use function array_keys;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_union;
use function get_debug_type;
use function implode;
use function is_array;
use function sprintf;
use function var_export;

/**
 * @type S3ClientConfiguration = array<Configuration::OPTION_*|'http', null|string|array<array-key, mixed>>
 */
final class ConfiguredS3Client implements S3ClientSource
{
    private ?S3Client $client = null;

    /**
     * @var array<Configuration::OPTION_*, null|string>
     */
    private readonly array $clientConfiguration;

    private readonly float|int $maxDuration;

    private readonly int $maxRetries;

    private readonly float|int $timeout;

    /**
     * @param S3ClientConfiguration $configuration kept verbatim: env, profile and IMDS re-resolve in every process; "http" holds timeout, max_duration and max_retries
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        #[SensitiveParameter]
        private readonly array $configuration,
    ) {
        $http = $configuration['http'] ?? [];

        if (!is_array($http)) {
            throw new InvalidArgumentException(sprintf(
                'S3 client "http" must be an array of timeout, max_duration and max_retries, got %s',
                get_debug_type($http),
            ));
        }

        $unknown = array_diff(array_keys($http), ['timeout', 'max_duration', 'max_retries']);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf('S3 client "http" holds unknown keys: %s. Allowed: timeout, max_duration, max_retries', implode(
                ', ',
                $unknown,
            )));
        }

        $limits = [...['timeout' => 30, 'max_duration' => 300, 'max_retries' => 3], ...$http];

        if (!type_union(type_integer(), type_float())->isValid($limits['timeout']) || $limits['timeout'] <= 0) {
            throw new InvalidArgumentException(sprintf('S3 client "http.timeout" must be a number above 0, got %s', var_export(
                $limits['timeout'],
                true,
            )));
        }

        if (
            !type_union(type_integer(), type_float())->isValid($limits['max_duration'])
            || $limits['max_duration'] < 0
        ) {
            throw new InvalidArgumentException(sprintf('S3 client "http.max_duration" must be 0 (no limit) or a number above 0, got %s', var_export(
                $limits['max_duration'],
                true,
            )));
        }

        if (!type_integer()->isValid($limits['max_retries']) || $limits['max_retries'] < 0) {
            throw new InvalidArgumentException(sprintf('S3 client "http.max_retries" must be an integer of 0 or more, got %s', var_export(
                $limits['max_retries'],
                true,
            )));
        }

        $clientConfiguration = $configuration;
        unset($clientConfiguration['http']);

        /** @var array<Configuration::OPTION_*, null|string> $clientConfiguration async-aws validates the options itself */
        $this->clientConfiguration = $clientConfiguration;
        $this->timeout = $limits['timeout'];
        $this->maxDuration = $limits['max_duration'];
        $this->maxRetries = $limits['max_retries'];
    }

    public function client(): S3Client
    {
        return $this->client ??= new S3Client(
            $this->clientConfiguration,
            null,
            new RetryableHttpClient(
                HttpClient::create(['timeout' => $this->timeout, 'max_duration' => $this->maxDuration]),
                new AwsRetryStrategy(AwsRetryStrategy::DEFAULT_RETRY_STATUS_CODES, 1000, 2.0, 0, 0.1),
                $this->maxRetries,
            ),
        );
    }

    /**
     * @return S3ClientConfiguration
     */
    public function normalize(): array
    {
        return $this->configuration;
    }
}
