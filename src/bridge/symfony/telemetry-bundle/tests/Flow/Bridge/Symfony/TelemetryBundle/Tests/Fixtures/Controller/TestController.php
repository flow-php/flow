<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller;

use Symfony\Component\HttpFoundation\{JsonResponse, Response};

final class TestController
{
    public function error() : Response
    {
        return new JsonResponse(['error' => 'not found'], 404);
    }

    public function exception() : Response
    {
        throw new \RuntimeException('Test exception');
    }

    public function index() : Response
    {
        return new JsonResponse(['status' => 'ok']);
    }
}
