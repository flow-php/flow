<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Psr\Http\Message\RequestInterface;

final class RequestEntriesFactory
{
    /**
     * @param RequestInterface $request
     *
     * @throws \JsonException
     * @throws InvalidArgumentException
     *
     * @return Row\Entries
     * @psalm-pure
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InvalidLiteralArgument
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedArgument
     */
    public function create(RequestInterface $request) : Row\Entries
    {
        $requestType = 'html';

        foreach ($request->getHeader('Accept') as $header) {
            if (\strpos('application/json', $header) !== false) {
                $requestType = 'json';
            }
        }

        $requestBody = $request->getBody();

        if ($requestBody->isReadable()) {
            if ($requestBody->isSeekable()) {
                $requestBody->seek(0);
            }

            $requestBodyContent = $requestBody->getContents();

            if ($requestBody->isSeekable()) {
                $requestBody->seek(0);
            }

            switch ($requestType) {
                case 'json':
                    if (\class_exists('Flow\ETL\Row\Entry\JsonEntry')) {
                        $requestBodyEntry = new Row\Entry\JsonEntry('request_body', \json_decode($requestBodyContent, true, 512, JSON_THROW_ON_ERROR));
                    } else {
                        $requestBodyEntry = new Row\Entry\StringEntry('request_body', $requestBodyContent);
                    }

                    break;

                default:
                    $requestBodyEntry = new Row\Entry\StringEntry('request_body', $requestBodyContent);

                    break;
            }
        } else {
            $requestBodyEntry = new Row\Entry\NullEntry('request_body');
        }

        return new Row\Entries(
            $requestBodyEntry,
            new Row\Entry\StringEntry('request_uri', (string) $request->getUri()),
            new Row\Entry\ArrayEntry('request_headers', $request->getHeaders()),
            new Row\Entry\StringEntry('request_protocol_version', $request->getProtocolVersion()),
            new Row\Entry\StringEntry('request_method', $request->getMethod()),
        );
    }
}
