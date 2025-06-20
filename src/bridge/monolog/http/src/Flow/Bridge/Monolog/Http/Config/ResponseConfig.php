<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Config;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Http\Sanitization\{Sanitizer, SanitizerFactory};

final readonly class ResponseConfig
{
    /**
     * @var array<string, array<string, mixed>|Sanitizer>
     */
    private array $sanitizers;

    /**
     * @param bool $withReasonPhrase
     * @param bool $withStatus
     * @param bool $withBody
     * @param int $bodySizeLimit
     * @param array<int> $withoutStatusCodes
     * @param array<string> $headers
     * @param array<string, array<string, mixed>|Sanitizer> $sanitizers
     */
    public function __construct(
        private bool $withReasonPhrase = true,
        private bool $withStatus = true,
        private bool $withBody = false,
        private int $bodySizeLimit = 1024 * 1024 * 32,
        private array $withoutStatusCodes = [],
        private array $headers = ['cache-control', 'location', 'set-cookie', 'server', 'expires', 'content-type', 'content-length', 'last-modified', 'kee-alive', 'referrer-policy', 'etag'],
        array $sanitizers = [],
    ) {
        $initializedSanitizers = [];

        foreach ($sanitizers as $key => $sanitizer) {
            if ($sanitizer instanceof Sanitizer) {
                $initializedSanitizers[$key] = $sanitizer;
            } elseif (\is_array($sanitizer)) {
                try {
                    $initializedSanitizers[$key] = SanitizerFactory::fromArray($sanitizer);
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException(\sprintf('Sanitizer for key "%s" could not be created from array: %s', $key, $e->getMessage()), 0, $e);
                }
            } else {
                throw new InvalidArgumentException(\sprintf('Sanitizer for key "%s" must be an instance of Sanitizer or an array that can be converted to a Sanitizer', $key));
            }
        }

        $this->sanitizers = $initializedSanitizers;
    }

    public function bodySizeLimit() : int
    {
        return $this->bodySizeLimit;
    }

    /**
     * @return array<int>
     */
    public function excludeStatusCodes() : array
    {
        return $this->withoutStatusCodes;
    }

    public function includeBody() : bool
    {
        return $this->withBody;
    }

    /**
     * @return array<string>
     */
    public function includeHeaders() : array
    {
        return $this->headers;
    }

    public function includeReasonPhrase() : bool
    {
        return $this->withReasonPhrase;
    }

    public function includeStatus() : bool
    {
        return $this->withStatus;
    }

    /**
     * @return array<string, Sanitizer>
     */
    public function sanitizers() : array
    {
        return $this->sanitizers;
    }
}
