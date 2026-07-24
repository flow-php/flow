<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Psr\Http\Message\RequestInterface;

final class RequestComparator
{
    public function equals(RequestInterface $left, RequestInterface $right): bool
    {
        return (
            $left->getMethod() === $right->getMethod()
            && (string) $left->getUri() === (string) $right->getUri()
            && $this->body($left) === $this->body($right)
        );
    }

    private function body(RequestInterface $request): string
    {
        $body = $request->getBody();

        if (!$body->isReadable()) {
            return '';
        }

        if ($body->isSeekable()) {
            $body->seek(0);
        }

        $content = $body->getContents();

        if ($body->isSeekable()) {
            $body->seek(0);
        }

        return $content;
    }
}
