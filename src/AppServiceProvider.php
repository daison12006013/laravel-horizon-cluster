<?php

namespace Daison\LaravelHorizonCluster;

use Daison\LaravelHorizonCluster\Mod;
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
     * All of the service bindings for Horizon.
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
}
