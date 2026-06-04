<?php

namespace Daison\LaravelHorizonCluster;

use Illuminate\Redis\Connections\Connection;
use Laravel\Horizon\AutoScaler;
use Laravel\Horizon\Contracts;
use Laravel\Horizon\Horizon;
use Laravel\Horizon\HorizonServiceProvider as Base;
use Laravel\Horizon\Listeners;
use Laravel\Horizon\Lock;
use Laravel\Horizon\Notifications;
use Laravel\Horizon\Repositories;
use Laravel\Horizon\Stopwatch;

class AppServiceProvider extends Base
{
    /**
     * All the service bindings for Horizon.
     *
     * @var array
     */
    public $serviceBindings = [
        // General services...
        AutoScaler::class,
        Contracts\HorizonCommandQueue::class => Mod\RedisHorizonCommandQueue::class,
        Listeners\TrimRecentJobs::class,
        Listeners\TrimFailedJobs::class,
        Listeners\TrimMonitoredJobs::class,
        Lock::class,
        Stopwatch::class,

        // Repository services...
        Contracts\JobRepository::class              => Mod\RedisJobRepository::class, // Repositories\RedisJobRepository::class,
        Contracts\MasterSupervisorRepository::class => Mod\RedisMasterSupervisorRepository::class, // Repositories\RedisMasterSupervisorRepository::class,
        Contracts\MetricsRepository::class          => Mod\RedisMetricsRepository::class, // Repositories\RedisMetricsRepository::class,
        Contracts\ProcessRepository::class          => Mod\RedisProcessRepository::class,
        Contracts\SupervisorRepository::class       => Mod\RedisSupervisorRepository::class, // Repositories\RedisSupervisorRepository::class
        Contracts\TagRepository::class              => Mod\RedisTagRepository::class, // Repositories\RedisTagRepository::class,
        Contracts\WorkloadRepository::class         => Repositories\RedisWorkloadRepository::class,

        // Notifications...
        Contracts\LongWaitDetectedNotification::class => Notifications\LongWaitDetected::class,
    ];

    /**
     * Compilation of parent::configure + Horizon::use
     * This change prevents downgrade from cluster to standby connection.
     * This solves the problem: if the first node is unavailable, the connection will not throw an exception,
     * but will connect to the next node.
     *
     * @return void
     */
    protected function configure(): void
    {
        $this->mergeConfigFrom(
            dirname((new \ReflectionClass(get_parent_class($this)))->getFileName()) . '/../config/horizon.php',
            'horizon'
        );

        $use = $this->normalizeConnectionName(config('horizon.use', 'default'));

        if (!config('horizon.reconnect_to_next_node_on_fail', false)) {
            Horizon::use($use);

            return;
        }

        if (! is_null($config = config("database.redis.clusters.$use"))) {
            $this->configureClusterConnection($config);

            return;
        }

        if (! is_null($config = config("database.redis.$use"))) {
            $this->configureStandaloneConnection($config);

            return;
        }

        throw new \Exception("Redis connection [$use] has not been configured.");
    }

    protected function normalizeConnectionName(string $connection): string
    {
        return str_starts_with($connection, 'clusters.')
            ? substr($connection, strlen('clusters.'))
            : $connection;
    }

    protected function configureClusterConnection(array $config): void
    {
        if (! method_exists(Connection::class, 'hasHashTag')) {
            $this->configureStandaloneConnection($config[0]);

            return;
        }

        $prefix = $this->ensureHashTaggedPrefix(config('horizon.prefix') ?: 'horizon:');

        $config['options']['prefix'] = $prefix;

        config(['horizon.prefix' => $prefix]);
        config(['database.redis.clusters.horizon' => $config]);
    }

    protected function configureStandaloneConnection(array $config): void
    {
        $config['options']['prefix'] = $prefix = config('horizon.prefix') ?: 'horizon:';

        config(['horizon.prefix' => $prefix]);
        config(['database.redis.horizon' => $config]);
    }

    protected function ensureHashTaggedPrefix(string $prefix): string
    {
        return Connection::hasHashTag($prefix) ? $prefix : '{'.$prefix.'}';
    }
}
