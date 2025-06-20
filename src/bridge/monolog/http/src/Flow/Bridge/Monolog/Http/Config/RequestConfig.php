<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Config;

use Flow\Bridge\Monolog\Http\Exception\InvalidArgumentException;
use Flow\Bridge\Monolog\Http\Sanitization\{Sanitizer, SanitizerFactory};

final readonly class RequestConfig
{
    /**
     * @var array<string, array<string, mixed>|Sanitizer>
     */
    private array $sanitizers;

    /**
     * @param bool $withMethod
     * @param bool $withUri
     * @param bool $withBody
     * @param int $bodySizeLimit
     * @param array<string> $headers
     * @param array<string, array<string, mixed>|Sanitizer> $sanitizers
     */
    public function __construct(
        private bool $withMethod = true,
        private bool $withUri = true,
        private bool $withBody = false,
        private int $bodySizeLimit = 1024 * 1024 * 32,
        private array $headers = ['host', 'accept', 'user-agent'],
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

    public function includeMethod() : bool
    {
        return $this->withMethod;
    }

    public function includeUri() : bool
    {
        return $this->withUri;
    }

    /**
     * @return array<string, Sanitizer>
     */
    public function sanitizers() : array
    {
        return $this->sanitizers;
    }
}
