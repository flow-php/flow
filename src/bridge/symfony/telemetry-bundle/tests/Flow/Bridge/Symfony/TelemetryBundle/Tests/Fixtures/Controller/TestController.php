<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller;

use RuntimeException;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Response;

final class TestController
{
    public function error(): Response
    {
        return new JsonResponse(['error' => 'not found'], 404);
    }

    public function exception(): Response
    {
        throw new RuntimeException('Test exception');
    }

    public function index(): Response
    {
        return new JsonResponse(['status' => 'ok']);
    }
}
