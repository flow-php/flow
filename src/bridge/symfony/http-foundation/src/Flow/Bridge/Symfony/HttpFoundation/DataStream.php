<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Response\{FlowBufferedResponse, FlowStreamedResponse};
use Flow\ETL\{Extractor, Transformation, Transformations};
use Symfony\Component\HttpFoundation\{HeaderUtils, Response};

/**
 * FlowStreamedResponse builder.
 */
final class DataStream
{
    /**
     * @var array<string, string>
     */
    private array $headers = [
        'Cache-Control' => 'no-store, no-cache, must-revalidate, private',
        'X-Accel-Buffering' => 'no', // provides support for Nginx
        'Pragma' => 'no-cache', // Backward compatibility for HTTP/1.0
    ];

    private int $status = Response::HTTP_OK;

    /**
     * @var array<Transformation>
     */
    private array $transformations = [];

    public function __construct(private readonly Extractor $extractor)
    {
    }

    public static function open(Extractor $extractor) : self
    {
        return new self($extractor);
    }

    /**
     * Set the filename for the response.
     * If the attachment flag is set to true, the response will be treated as an attachment meaning that
     * the browser will prompt the user to download the file.
     */
    public function as(string $name, bool $attachment = true) : self
    {
        $this->headers['Content-Disposition'] = HeaderUtils::makeDisposition(
            $attachment ? HeaderUtils::DISPOSITION_ATTACHMENT : HeaderUtils::DISPOSITION_INLINE,
            $name
        );

        return $this;
    }

    /**
     * Set additional headers.
     * Headers are merged with the default headers.
     *
     * @param array<string, string> $headers
     */
    public function headers(array $headers) : self
    {
        $this->headers = array_merge($this->headers, $headers);

        return $this;
    }

    /**
     * Create regular response where whole dataset is loaded into the memory.
     * It's highly recommended to use limit transformation to avoid loading entire dataset into the memory.
     * Some extractors like Parquet/Elasticsearch/Doctrine allows also for setting offset directly on the extractor.
     */
    public function response(Output $output) : FlowBufferedResponse
    {
        $this->headers['Content-Type'] = $output->type()->toContentTypeHeader();

        return new FlowBufferedResponse(
            $this->extractor,
            $output,
            \count($this->transformations) ? new Transformations(...$this->transformations) : new Transformations(),
            $this->status,
            $this->headers
        );
    }

    /**
     * Set the HTTP status code. Default is 200.
     */
    public function status(int $status) : self
    {
        $this->status = $status;

        return $this;
    }

    /**
     * Send the data stream to the output.
     */
    public function streamedResponse(Output $output) : FlowStreamedResponse
    {

        $this->headers['Content-Type'] = $output->type()->toContentTypeHeader();

        return new FlowStreamedResponse(
            $this->extractor,
            $output,
            \count($this->transformations) ? new Transformations(...$this->transformations) : new Transformations(),
            $this->status,
            $this->headers
        );
    }

    /**
     * Apply transformations to the data stream.
     * Transformations are applied in the order they are passed.
     * Transformations are applied on the fly, while streaming the data, this means
     * that any resource expensive transformations like for example aggregations or sorting
     * might significantly slow down the streaming process or even cause out of memory errors.
     */
    public function transform(Transformation ...$transformations) : self
    {
        $this->transformations = $transformations;

        return $this;
    }

    /**
     * Remove a specific header if it exists.
     * If the header does not exist, nothing happens.
     */
    public function withoutHeader(string $name) : self
    {
        if (\array_key_exists($name, $this->headers)) {
            unset($this->headers[$name]);
        }

        return $this;
    }
}
