<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\ProfilerController;
use Flow\PostgreSql\Client\Debug\QueryLog;
use Flow\PostgreSql\Client\Debug\RecordedQuery;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Tests\Mother\FakeClient;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\HttpKernel\Profiler\Profile;
use Symfony\Component\HttpKernel\Profiler\Profiler;
use Twig\Environment;

#[CoversClass(ProfilerController::class)]
final class ProfilerControllerTest extends TestCase
{
    public function test_renders_the_plan_for_an_explainable_query(): void
    {
        $controller = $this->controller(
            $this->profilerWith($this->collectorWith('SELECT * FROM users WHERE id = $1', [7])),
            new ServiceLocator(['default' => static fn(): FakeClient => new FakeClient()]),
            renders: '<div class="flow-pg-explain">plan</div>',
        );

        $response = $controller->explainAction('tok', 'default', 0);

        static::assertSame(200, $response->getStatusCode());
        static::assertStringContainsString('flow-pg-explain', (string) $response->getContent());
    }

    public function test_returns_message_when_profile_missing(): void
    {
        $profiler = $this->createStub(Profiler::class);
        $profiler->method('loadProfile')->willReturn(null);

        $response = $this->controller($profiler, new ServiceLocator([]))->explainAction('tok', 'default', 0);

        static::assertStringContainsString('does not exist', (string) $response->getContent());
    }

    public function test_returns_message_when_collector_absent(): void
    {
        $profiler = $this->createStub(Profiler::class);
        $profiler->method('loadProfile')->willReturn(new Profile('tok'));

        $response = $this->controller($profiler, new ServiceLocator([]))->explainAction('tok', 'default', 0);

        static::assertStringContainsString('does not exist', (string) $response->getContent());
    }

    public function test_returns_message_when_query_index_unknown(): void
    {
        $controller = $this->controller($this->profilerWith($this->collectorWith('SELECT 1', [])), new ServiceLocator([
            'default' => static fn(): FakeClient => new FakeClient(),
        ]));

        static::assertStringContainsString(
            'does not exist',
            (string) $controller->explainAction('tok', 'default', 99)->getContent(),
        );
    }

    public function test_returns_message_when_query_is_not_explainable(): void
    {
        $controller = $this->controller(
            $this->profilerWith($this->collectorWith('SET search_path TO public', [])),
            new ServiceLocator(['default' => static fn(): FakeClient => new FakeClient()]),
        );

        static::assertStringContainsString(
            'cannot be explained',
            (string) $controller->explainAction('tok', 'default', 0)->getContent(),
        );
    }

    public function test_returns_message_when_connection_has_no_client(): void
    {
        $controller = $this->controller(
            $this->profilerWith($this->collectorWith('SELECT 1', [])),
            new ServiceLocator([]),
        );

        static::assertStringContainsString(
            'cannot be explained',
            (string) $controller->explainAction('tok', 'default', 0)->getContent(),
        );
    }

    public function test_returns_message_when_explain_fails(): void
    {
        $failing = new FakeClient();
        $failing->failNextQuery(QueryException::executionFailed('SELECT 1', PostgreSqlError::unknown('boom')));

        $controller = $this->controller($this->profilerWith($this->collectorWith('SELECT 1', [])), new ServiceLocator([
            'default' => static fn(): FakeClient => $failing,
        ]));

        static::assertStringContainsString(
            'error occurred',
            (string) $controller->explainAction('tok', 'default', 0)->getContent(),
        );
    }

    /**
     * @param list<mixed> $parameters
     */
    private function collectorWith(string $statement, array $parameters): FlowPostgreSqlDataCollector
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery($statement, $parameters, 1.0, 1, false, null));
        $collector = new FlowPostgreSqlDataCollector($log, includeParameters: true);
        $collector->lateCollect();

        return $collector;
    }

    private function profilerWith(FlowPostgreSqlDataCollector $collector): Profiler
    {
        $profile = new Profile('tok');
        $profile->addCollector($collector);

        $profiler = $this->createStub(Profiler::class);
        $profiler->method('loadProfile')->willReturn($profile);

        return $profiler;
    }

    private function controller(Profiler $profiler, ServiceLocator $clients, string $renders = ''): ProfilerController
    {
        $twig = $this->createStub(Environment::class);
        $twig->method('render')->willReturn($renders);

        return new ProfilerController($twig, $clients, $profiler);
    }
}
