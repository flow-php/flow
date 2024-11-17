<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class ListBlobOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?string $delimiter = null;

    /**
     * @var null|array<OptionInclude>
     */
    private ?array $include = null;

    private ?string $marker = null;

    private ?int $maxResults = null;

    private ?string $prefix = null;

    private ?string $requestId = null;

    private ?OptionShowOnly $showOnly = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    public function __construct()
    {
    }

    public function toHeaders() : array
    {
        $headers = [];

        $headers['user-agent'] = $this->userAgentHeader();
        $headers['x-ms-version'] = $this->version;

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        return $headers;
    }

    public function toURIParameters() : array
    {
        $uriParameters = [];

        if ($this->prefix !== null) {
            $uriParameters['prefix'] = $this->prefix;
        }

        if ($this->delimiter !== null) {
            $uriParameters['delimiter'] = $this->delimiter;
        }

        if ($this->maxResults !== null) {
            $uriParameters['maxresults'] = $this->maxResults;
        }

        if ($this->marker !== null) {
            $uriParameters['marker'] = $this->marker;
        }

        if ($this->include !== null) {
            $uriParameters['include'] = \array_map(static fn (OptionInclude $include) => $include->value, $this->include);
        }

        if ($this->showOnly !== null) {
            $uriParameters['showonly'] = $this->showOnly->value;
        }

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = $this->timeoutSeconds;
        }

        return $uriParameters;
    }

    public function withDelimiter(string $delimiter) : self
    {
        $this->delimiter = $delimiter;

        return $this;
    }

    public function withInclude(OptionInclude ...$include) : self
    {
        $this->include = $include;

        return $this;
    }

    public function withMarker(string $marker) : self
    {
        $this->marker = $marker;

        return $this;
    }

    public function withMaxResults(int $maxResults) : self
    {
        $this->maxResults = $maxResults;

        return $this;
    }

    public function withPrefix(string $prefix) : self
    {
        $this->prefix = $prefix;

        return $this;
    }

    public function withRequestId(string $requestId) : self
    {
        $this->requestId = $requestId;

        return $this;
    }

    public function withShowOnly(OptionShowOnly $showOnly) : self
    {
        $this->showOnly = $showOnly;

        return $this;
    }

    public function withTimeoutSeconds(int $timeoutSeconds) : self
    {
        $this->timeoutSeconds = $timeoutSeconds;

        return $this;
    }

    public function withVersion(string $version) : self
    {
        $this->version = $version;

        return $this;
    }
}
