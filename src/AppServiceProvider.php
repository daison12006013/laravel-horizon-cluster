<?php

namespace Daison\LaravelHorizonCluster;

use Laravel\Horizon\AutoScaler;
use Laravel\Horizon\Contracts;
use Laravel\Horizon\HorizonServiceProvider as Base;
use Laravel\Horizon\Listeners;
use Laravel\Horizon\Lock;
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
        if (!config('horizon.reconnect_to_next_node_on_fail', false)) {
            parent::configure();

            return;
        }

        $this->mergeConfigFrom(
            dirname((new \ReflectionClass(get_parent_class($this)))->getFileName()) . '/../config/horizon.php',
            'horizon'
        );

        $use = config('horizon.use', 'default');

        if (
            is_null($config = config("database.redis.clusters.$use"))
            && is_null($config = config("database.redis.$use"))
        ) {
            throw new \Exception("Redis connection [$use] has not been configured.");
        }

        $config['options']['prefix'] = config('horizon.prefix') ?: 'horizon:';
        config(['database.redis.horizon' => $config]);
    }
}
